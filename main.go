package main

import (
	"flag"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	corev1 "k8s.io/api/core/v1"

	"github.com/golang/glog"
)

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
	controller := newPvcController(*kubeconfig)
	var stopCh <-chan struct{}
	controller.Run(2, stopCh)
}

type pvcController struct {
	kubeClient *kubernetes.Clientset
	controller cache.Controller
	podStore   cache.Store
	podsQueue  workqueue.RateLimitingInterface
}

func newPvcController(kubeconfig string) *pvcController {
	pvc := &pvcController{
		kubeClient: getClientsetOrDie(kubeconfig),
		podsQueue:  workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "pods"),
	}

	pvc.podStore, pvc.controller = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				return pvc.kubeClient.Core().Pods(v1.NamespaceAll).List(options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				return pvc.kubeClient.Core().Pods(v1.NamespaceAll).Watch(options)
			},
		},
		&corev1.Pod{},
		// resync is not needed
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*corev1.Pod)
				glog.Infof("add %s/%s", pod.Namespace, pod.Name)
				if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err == nil {
					pvc.podsQueue.Add(key)
				}
			},
			UpdateFunc: func(old, new interface{}) {
				newpod := new.(*corev1.Pod)
				oldpod := old.(*corev1.Pod)
				glog.Infof("update old: %#v \n---- new: %#v", *oldpod, *newpod)
				if key, err := cache.MetaNamespaceKeyFunc(new); err != nil {
					pvc.podsQueue.Add(key)
				}
			},
			DeleteFunc: func(obj interface{}) {
				pod := obj.(*corev1.Pod)
				glog.Infof("delete %s/%s", pod.Namespace, pod.Name)
				if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err == nil {
					pvc.podsQueue.Add(key)
				}
			},
		},
	)

	return pvc
}

func (pvc *pvcController) Run(workers int, stopCh <-chan struct{}) {

	fmt.Println("Starting pvc controller")
	go pvc.controller.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, pvc.controller.HasSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(pvc.runWorker, time.Second, stopCh)
	}

	<-stopCh
	fmt.Printf("Shutting down pvc Controller")
	pvc.podsQueue.ShutDown()
}

func (pvc *pvcController) runWorker() {

	workFunc := func() bool {
		key, quit := pvc.podsQueue.Get()
		if quit {
			return true
		}
		defer pvc.podsQueue.Done(key)

		obj, exists, err := pvc.podStore.GetByKey(key.(string))
		if !exists {
			fmt.Printf("Pod has been deleted %v\n", key)
			return false
		}
		if err != nil {
			fmt.Printf("cannot get pod: %v\n", key)
			return false
		}

		pod := obj.(*corev1.Pod)

		fmt.Println(pod.Name)
		//TODO update pvc annotation
		return false
	}
	for {
		if quit := workFunc(); quit {
			fmt.Printf("worker shutting down")
			return
		}
	}
}
