package main

import (
	"flag"
	"fmt"
	"time"
	"strings"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/labels"
)

const USEBY = "kubernetes-admin.caicloud.io/used-by"

// PodController is responsible for performing actions dependent upon a Pod phase
type PodController struct {
	kubeClient   *kubernetes.Clientset
	listerSynced cache.InformerSynced
	queue        workqueue.RateLimitingInterface
	podInformer  coreinformers.PodInformer
}

func getClientsetOrDie(kubeconfig string) *kubernetes.Clientset {
	// Create the client config. Use kubeconfig if given, otherwise assume in-cluster.
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	return clientset
}

func main() {
	kubeconfig := flag.String("kubeconfig", "", "Path to a kube config. Only required if out-of-cluster.")

	flag.Parse()
	kubeClient := getClientsetOrDie(*kubeconfig)

	sharedInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	var stop <-chan struct{}
	sharedInformerFactory.Start(stop)

	controller := NewPodController(kubeClient, sharedInformerFactory.Core().V1().Pods(), 0)
	var stopCh <-chan struct{}
	controller.Run(2, stopCh)
}

// NewPodController creates a new PodController
func NewPodController(
	kubeClient *kubernetes.Clientset,
	PodInformer coreinformers.PodInformer,
	resyncPeriod time.Duration,
) *PodController {

	// create the controller so we can inject the enqueue function
	PodController := &PodController{
		kubeClient: kubeClient,
		queue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Pod"),
	}

	// configure the Pod informer event handlers
	PodInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				Pod := obj.(*corev1.Pod)
				PodController.enqueuePod(Pod)
			},
			UpdateFunc: func(old, new interface{}) {
				Pod := new.(*corev1.Pod)
				PodController.enqueuePod(Pod)
			},
			DeleteFunc: func(obj interface{}) {
				Pod := obj.(*corev1.Pod)
				PodController.enqueuePod(Pod)
			},
		},
		resyncPeriod,
	)
	PodController.podInformer = PodInformer
	PodController.listerSynced = PodInformer.Informer().HasSynced
	return PodController
}

// enqueuePod adds an object to the controller work queue
// obj could be an *v1.Pod, or a DeletionFinalStateUnknown item.
func (nm *PodController) enqueuePod(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	pod := obj.(*corev1.Pod)
	if checkPodHasPVC(pod) == false {
		return
	}
	nm.queue.Add(key)
}

// worker processes the queue of Pod objects.
// Each Pod can be in the queue at most once.
// The system ensures that no two workers can process
// the same Pod at the same time.
func (nm *PodController) worker() {
	workFunc := func() bool {
		key, quit := nm.queue.Get()
		if quit {
			return true
		}
		defer nm.queue.Done(key)

		err := nm.syncPodFromKey(key.(string))
		if err == nil {
			// no error, forget this entry and return
			nm.queue.Forget(key)
			return false
		}
		nm.queue.AddRateLimited(key)
		utilruntime.HandleError(err)

		return false
	}

	for {
		quit := workFunc()

		if quit {
			return
		}
	}
}

// syncPodFromKey looks for a Pod with the specified key in its store and synchronizes it
func (nm *PodController) syncPodFromKey(key string) (err error) {
	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing Pod %q (%v)", key, time.Now().Sub(startTime))
	}()

	obj, exist, err := nm.podInformer.Informer().GetStore().GetByKey(key)
	if !exist {
		glog.V(4).Infof("Pod has been deleted ", key)
		return nil
	}
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Unable to retrieve pod %v from store: %v", key, err))
		return err
	}
	pod := obj.(*corev1.Pod)
	glog.V(4).Infof("Deleting Pod releases", pod)
	return nm.syncHandle(pod)
}

// syncHandle maintain pvc useby field
func (pvc *PodController) syncHandle(pod *corev1.Pod) error {
	if pod.DeletionTimestamp != nil {
		err, p := getPvcByPod(pod, pvc.kubeClient)
		if err != nil {
			utilruntime.HandleError(err)
			return err
		}
		pvcAnnotation := getPvcAnnotation(p)
		if pvcAnnotation == nil {
			return nil
		}
		delete(pvcAnnotation, pod.Name)
		if err := updatePvcAnnotation(pvcAnnotation, pvc.kubeClient, p); err != nil {
			utilruntime.HandleError(err)
			return err
		}
		return nil
	}
	err, p := getPvcByPod(pod, pvc.kubeClient)
	if err != nil {
		utilruntime.HandleError(err)
		return err
	}
	pvcAnnotation := getPvcAnnotation(p)
	pvcAnnotation[pod.Name] = 0
	if err := updatePvcAnnotation(pvcAnnotation, pvc.kubeClient, p); err != nil {
		utilruntime.HandleError(err)
		return err
	}
	return nil
}

// initHandle clean all pvc annotaion
func (pvc *PodController) initHandle() error {
	res, err := pvc.podInformer.Lister().List(labels.NewSelector())
	if err != nil {
		glog.Error(err)
		return err
	}
	for _, pod := range res {
		if checkPodHasPVC(pod) == true {
			err, p := getPvcByPod(pod, pvc.kubeClient)
			if err != nil {
				glog.Error(err)
				return err
			}
			p.Annotations[USEBY] = ""
			_, err = pvc.kubeClient.PersistentVolumeClaims(p.Namespace).Update(p)
			if err != nil {
				glog.Info(err)
				return err
			}
			glog.Info("clean sucessed")
		}
	}
	return nil
}

// getPvcByPod get pvc by special pod
func getPvcByPod(pod *corev1.Pod, clientset *kubernetes.Clientset) (error, *corev1.PersistentVolumeClaim) {
	if len(pod.Spec.Volumes) != 0 {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				pvc, err := clientset.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(volume.PersistentVolumeClaim.ClaimName, v1.GetOptions{})
				if err != nil {
					glog.Info(err)
					return err, nil
				}
				return nil, pvc
			}
		}
	}
	return fmt.Errorf("can not find pvc in this pod"), nil
}

// getPvcAnnotation get pvc annotation
func getPvcAnnotation(pvc *corev1.PersistentVolumeClaim) (map[string]int) {
	podNameMap := make(map[string]int, 0)
	if values, ok := pvc.Annotations[USEBY]; values != "" && ok {
		value := strings.Split(values, ",")
		for _, v := range value {
			podNameMap[v] = 0
		}
		return podNameMap
	}
	return podNameMap
}

// updatePvcAnnotation update pvc annotation by you special pod name
func updatePvcAnnotation(annotation map[string]int, clientset *kubernetes.Clientset, pvc *corev1.PersistentVolumeClaim) error {
	sl := []string{}
	for value, _ := range annotation {
		sl = append(sl, value)
	}

	pvc.Annotations[USEBY] = strings.Join(sl, ",")
	pvc, err := clientset.PersistentVolumeClaims(pvc.Namespace).Update(pvc)
	if err != nil {
		glog.Info(err)
		return err
	}
	glog.Infof("updated pvc %s annotation:", pvc.Name, pvc.Annotations)
	return nil
}

// checkPodHasPVC check pod is not use pvc
func checkPodHasPVC(pod *corev1.Pod) bool {
	if len(pod.Spec.Volumes) != 0 {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				return true
			}
		}
	}
	return false
}

// Run starts observing the system with the specified number of workers.
func (pvc *PodController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer pvc.queue.ShutDown()

	go pvc.podInformer.Informer().Run(stopCh)
	fmt.Println("Starting Pod controller")
	if !cache.WaitForCacheSync(stopCh, pvc.listerSynced) {
		return
	}
	if err := pvc.initHandle(); err != nil {
		glog.Error("init handle fail, may cannot connect apiserver")
	}
	fmt.Println("Starting workers")
	for i := 0; i < workers; i++ {
		go wait.Until(pvc.worker, time.Second, stopCh)
	}
	<-stopCh
	glog.V(1).Infof("Shutting down")
}
