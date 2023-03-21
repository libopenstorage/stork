package main

import (
	"fmt"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/portworx/sched-ops/k8s/core"
	// "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"time"
)

func main() {
	var ns string
	var details string
	if len(os.Args) >= 2 {
		ns = os.Args[1]
	}

	configLoadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configLoadingRules.ExplicitPath = "/tmp/config"
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(configLoadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		fmt.Printf("Error getting kubeconfig: %v", err)
	}

	r := &resourcecollector.ResourceCollector{}
	if err := r.Init(config); err != nil {
		fmt.Printf("Error initializing ResourceCollector: %v", err)
	}
	// Get all namespace and exclude kube-system
	k8sOps := core.Instance()
	k8sOps.SetConfig(config)
	total := 0
	count := 0
	nsList := []string{}
	if len(ns) == 0 {
		namespaces, err := core.Instance().ListNamespaces(nil)
		if err != nil {
			fmt.Printf("Error in getting namespace list: %v", err)
		}
		nsList = make([]string, len(namespaces.Items)-1)

		for _, ns := range namespaces.Items {
			if ns.GetName() == "kube-system" {
				continue
			}
			nsList[count] = ns.GetName()
			count++
			// fmt.Printf("%v\n", ns.GetName())
		}
		fmt.Printf("Total namespace count: [%v]\n", len(nsList))
	} else {
		nsList = []string{ns}
	}

	fmt.Printf("Namespace list :[%+v]\n", len(nsList))
	/*label := map[string]string {
	                   os.Args[1]:os.Args[2],
	}*/
	label := make(map[string]string)

	// Calling GetResource
	var optionalResourceTypes []string
	optionalResourceTypes = make([]string, 0)
	optionalResourceTypes = append(optionalResourceTypes, "Jobs")

	start := time.Now()
	resourceCollectorOpts := resourcecollector.Options{}
	objects, err := r.GetMiniResources(nsList, label, nil, optionalResourceTypes, true, resourceCollectorOpts)
	if err != nil {
		fmt.Printf("Error in GetResource: %v", err)
	}
	end := time.Now()
	fmt.Printf("Time taken by GetResource: [%v]\n", end.Sub(start))

	total += len(objects)
	fmt.Printf("Total objects: [%v]\n", total)
	if len(os.Args) >= 3 {
		details = os.Args[2]
		if len(details) != 0 && details == "dump" {
			for _, object := range objects {
				/*
				   metadata, err := meta.Accessor(object)
				   if err != nil {
				           fmt.Printf("Error in meta.Accessor: %v", err)
				   }
				   gvk := object.GetObjectKind().GroupVersionKind()
				   fmt.Printf("%v\t\t%v\t\t%v\n", metadata.GetNamespace(), gvk.Kind, metadata.GetName())
				*/
				fmt.Printf("the object is %+v", object)
			}
		}
	}
}
