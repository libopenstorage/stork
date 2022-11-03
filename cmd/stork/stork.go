package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	stork_driver "github.com/libopenstorage/stork/drivers"
	"github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/aws"
	_ "github.com/libopenstorage/stork/drivers/volume/azure"
	_ "github.com/libopenstorage/stork/drivers/volume/csi"
	_ "github.com/libopenstorage/stork/drivers/volume/gcp"
	_ "github.com/libopenstorage/stork/drivers/volume/kdmp"
	_ "github.com/libopenstorage/stork/drivers/volume/linstor"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/apis"
	"github.com/libopenstorage/stork/pkg/applicationmanager"
	"github.com/libopenstorage/stork/pkg/cache"
	"github.com/libopenstorage/stork/pkg/clusterdomains"
	"github.com/libopenstorage/stork/pkg/dbg"
	"github.com/libopenstorage/stork/pkg/extender"
	"github.com/libopenstorage/stork/pkg/groupsnapshot"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/metrics"
	"github.com/libopenstorage/stork/pkg/migration"
	"github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/libopenstorage/stork/pkg/monitor"
	"github.com/libopenstorage/stork/pkg/objectcontroller"
	"github.com/libopenstorage/stork/pkg/pluralmap"
	"github.com/libopenstorage/stork/pkg/pvcwatcher"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/snapshot"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/libopenstorage/stork/pkg/webhookadmission"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/controllers/dataexport"
	"github.com/portworx/kdmp/pkg/controllers/resourceexport"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/jobratelimit"
	kdmpversion "github.com/portworx/kdmp/pkg/version"
	schedops "github.com/portworx/sched-ops/k8s/core"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	api_v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	core_v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	defaultLockObjectName      = "stork"
	defaultLockObjectNamespace = "kube-system"
	defaultAdminNamespace      = "kube-system"
	storkVersion               = "version"
	cmName                     = "stork-version"
	eventComponentName         = "stork"
	debugFilePath              = "/var/cores"
	awsKopiaExecutorImage      = "709825985650.dkr.ecr.us-east-1.amazonaws.com/portworx/kopiaexecutor"
	awsKopiaExecutorImageTag   = "1.0.0-a345bb2"
	awsMarketPlace             = "aws"
)

var ext *extender.Extender
var webhook *webhookadmission.Controller

func main() {
	// Parse empty flags to suppress warnings from the snapshotter which uses
	// glog
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		log.Warnf("Error parsing flag: %v", err)
	}
	err = flag.Set("logtostderr", "true")
	if err != nil {
		log.Fatalf("Error setting glog flag: %v", err)
	}

	app := cli.NewApp()
	app.Name = "stork"
	app.Usage = "STorage Orchestartor Runtime for Kubernetes (STORK)"
	app.Version = version.Version
	app.Action = run

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "Enable verbose logging",
		},
		cli.StringFlag{
			Name:  "driver,d",
			Usage: "Storage driver name",
		},
		cli.BoolTFlag{
			Name:  "leader-elect",
			Usage: "Enable leader election (default: true)",
		},
		cli.StringFlag{
			Name:  "lock-object-name",
			Usage: "Name for the lock object",
			Value: defaultLockObjectName,
		},
		cli.StringFlag{
			Name:  "lock-object-namespace",
			Usage: "Namespace for the lock object",
			Value: defaultLockObjectNamespace,
		},
		cli.BoolTFlag{
			Name:  "snapshotter",
			Usage: "Enable snapshotter (default: true)",
		},
		cli.BoolTFlag{
			Name:  "extender",
			Usage: "Enable scheduler extender for hyperconvergence (default: true)",
		},
		cli.BoolTFlag{
			Name:  "health-monitor",
			Usage: "Enable health monitoring of the storage driver (default: true)",
		},
		cli.Int64Flag{
			Name:  "health-monitor-interval",
			Value: 120,
			Usage: "The interval in seconds to monitor the health of the storage driver (min: 30)",
		},
		cli.BoolTFlag{
			Name:  "migration-controller",
			Usage: "Start the migration controller (default: true)",
		},
		cli.BoolTFlag{
			Name:  "application-controller",
			Usage: "Start the controllers for managing applications (default: true)",
		},
		cli.BoolFlag{
			Name:  "px-object-controller",
			Usage: "Start the px object controller.",
		},
		cli.StringFlag{
			Name:  "admin-namespace",
			Value: defaultAdminNamespace,
			Usage: "Namespace to be used by a cluster admin which can migrate and backup all other namespaces",
		},
		cli.StringFlag{
			Name:  "migration-admin-namespace",
			Value: defaultAdminNamespace,
			Usage: "Namespace to be used by a cluster admin which can migrate all other namespaces (Deprecated, please use admin-namespace)",
		},
		cli.BoolTFlag{
			Name:  "cluster-domain-controllers",
			Usage: "Start the cluster domain controllers (default: true)",
		},
		cli.BoolTFlag{
			Name:  "pvc-watcher",
			Usage: "Start the controller to monitor PVC creation and deletions (default: true)",
		},
		cli.BoolTFlag{
			Name:  "webhook-controller",
			Usage: "Enable webhook controller to start driver apps with scheduler as stork (default: true)",
		},
		cli.StringFlag{
			Name:  "webhook-skip-resources-annotation",
			Usage: "Application annotation to be used to disable auto updating app scheduler as stork",
		},
		cli.BoolTFlag{
			Name:  "enable-metrics",
			Usage: "Enable stork metrics collection for stork resources (default: true)",
		},
		cli.Int64Flag{
			Name:  "application-backup-sync-interval",
			Value: 10,
			Usage: "The interval in seconds to sync reconcilers (default: 10 seconds)",
		},
		cli.IntFlag{
			Name:  "k8s-api-qps",
			Value: 1000,
			Usage: "Restrict number of k8s API requests from stork (default: 100 QPS)",
		},
		cli.IntFlag{
			Name:  "k8s-api-burst",
			Value: 2000,
			Usage: "Restrict number of k8s API requests from stork (default: 100 Burst)",
		},
		cli.BoolTFlag{
			Name:  "kdmp-controller",
			Usage: "Start the kdmp controller (default: true)",
		},
		cli.IntFlag{
			Name:  "migration-max-threads",
			Value: 4,
			Usage: "Max threads for apply resources during migration (default: 4)",
		},
		cli.BoolTFlag{
			Name:  controllers.ResourceTransformationControllerName,
			Usage: "Start the resource transformation controller (default: true)",
		},
	}

	// Export USE_GKE_GCLOUD_AUTH_PLUGIN=True
	log.Warnf("Export USE_GKE_GCLOUD_AUTH_PLUGIN=True")
	os.Setenv("USE_GKE_GCLOUD_AUTH_PLUGIN", "True")

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error starting stork: %v", err)
	}
}

func run(c *cli.Context) {
	dbg.Init(c.App.Name, debugFilePath)

	log.Infof("Starting stork version %v", version.Version)
	// create configmap with stork version details
	cm := &api_v1.ConfigMap{}
	cm.Name = cmName
	cm.Namespace = defaultAdminNamespace
	cm.Data = make(map[string]string)
	cm.Data[storkVersion] = version.Version
	// ConfigMap create/update op should not be blocking operation
	_, err := schedops.Instance().CreateConfigMap(cm)
	if k8s_errors.IsAlreadyExists(err) {
		_, err := schedops.Instance().UpdateConfigMap(cm)
		if err != nil {
			log.Warnf("unable to create stork version configmap: %v", err)
		}
	} else if err != nil {
		log.Warnf("Unable to create stork version configmap: %v", err)
	}
	// create configmap to store stork admin namespace details.
	storkControllerCm := &api_v1.ConfigMap{}
	storkControllerCm.Name = k8sutils.StorkControllerConfigMapName
	storkControllerCm.Namespace = defaultAdminNamespace
	storkControllerCm.Data = make(map[string]string)
	storkControllerCm.Data[k8sutils.AdminNsKey] = c.String("admin-namespace")
	// get the stork deployment namespace
	storkPodNs, err := k8sutils.GetStorkPodNamespace()
	if err != nil {
		log.Warnf("unable to get the stork pod namespace: %v", err)
	}
	// get the service account name of stork deployment
	serviceAccountName, err := k8sutils.GetServiceAccountFromDeployment(k8sutils.StorkDeploymentName, storkPodNs)
	if err != nil {
		log.Warnf("unable to get the stork service account: %v", err)
	}

	storkControllerCm.Data[k8sutils.StorkServiceAccount] = serviceAccountName
	storkControllerCm.Data[k8sutils.DeployNsKey] = storkPodNs

	_, err = schedops.Instance().CreateConfigMap(storkControllerCm)
	if k8s_errors.IsAlreadyExists(err) {
		_, err := schedops.Instance().UpdateConfigMap(storkControllerCm)
		if err != nil {
			log.Warnf("unable to update stork controller configmap: %v", err)
		}
	} else if err != nil {
		log.Warnf("Unable to create stork controller configmap: %v", err)
	}
	marketPlace := os.Getenv("MARKET_PLACE")
	kdmpConfig := &api_v1.ConfigMap{}
	kdmpConfig.Name = stork_driver.KdmpConfigmapName
	kdmpConfig.Namespace = stork_driver.KdmpConfigmapNamespace
	kdmpConfig.Data = make(map[string]string)
	kdmpConfig.Data[drivers.KopiaExecutorRequestCPU] = drivers.DefaultKopiaExecutorRequestCPU
	kdmpConfig.Data[drivers.KopiaExecutorRequestMemory] = drivers.DefaultKopiaExecutorRequestMemory
	kdmpConfig.Data[drivers.KopiaExecutorLimitCPU] = drivers.DefaultKopiaExecutorLimitCPU
	kdmpConfig.Data[drivers.KopiaExecutorLimitMemory] = drivers.DefaultKopiaExecutorLimitMemory
	kdmpConfig.Data[drivers.KopiaExecutorImageSecretKey] = ""
	kdmpConfig.Data[snapshotter.SnapshotTimeoutKey] = ""
	if marketPlace == awsMarketPlace {
		kdmpConfig.Data[drivers.KopiaExecutorImageKey] = strings.Join([]string{awsKopiaExecutorImage, awsKopiaExecutorImageTag}, ":")
	} else {
		kdmpConfig.Data[drivers.KopiaExecutorImageKey] = strings.Join([]string{drivers.KopiaExecutorImage, kdmpversion.Get().GitVersion}, ":")
	}
	kdmpConfig.Data[jobratelimit.BackupJobLimitKey] = strconv.Itoa(jobratelimit.DefaultBackupJobLimit)
	kdmpConfig.Data[jobratelimit.RestoreJobLimitKey] = strconv.Itoa(jobratelimit.DefaultRestoreJobLimit)
	kdmpConfig.Data[jobratelimit.DeleteJobLimitKey] = strconv.Itoa(jobratelimit.DefaultDeleteJobLimit)
	kdmpConfig.Data[jobratelimit.MaintenanceJobLimitKey] = strconv.Itoa(jobratelimit.DefaultMaintenanceJobLimit)
	// ConfigMap create failure should not fail the bring up
	_, err = schedops.Instance().CreateConfigMap(kdmpConfig)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		log.Warnf("Unable to create kdmp config configmap: %v", err)
	}
	storkConfig := &api_v1.ConfigMap{}
	storkConfig.Name = k8sutils.StorkConfigMapName
	storkConfig.Namespace = k8sutils.DefaultAdminNamespace
	storkConfig.Data = make(map[string]string)
	storkConfig.Data[k8sutils.ObjectLockIncrBackupCountKey] = ""
	// ConfigMap create failure should not fail the bring up
	_, err = schedops.Instance().CreateConfigMap(storkConfig)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		log.Warnf("Unable to create stork config configmap: %v", err)
	}

	driverName := c.String("driver")

	verbose := c.Bool("verbose")
	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	qps := float32(c.Int("k8s-api-qps"))
	burst := c.Int("k8s-api-burst")

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Error getting cluster config: %v", err)
	}

	if qps > 0 {
		config.QPS = qps
	}
	if burst > 0 {
		config.Burst = burst
	}
	k8sClient, err := clientset.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error getting client, %v", err)
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&core_v1.EventSinkImpl{Interface: k8sClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, api_v1.EventSource{Component: eventComponentName})

	// Create operator-sdk manager that will manage all controllers.
	// Setup the controller manager before starting any watches / other controllers
	mgr, err := manager.New(config, manager.Options{})
	if err != nil {
		log.Fatalf("Setup controller manager: %v", err)
	}

	mgrCtx := context.Background()
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			log.Fatalf("Controller manager: %v", err)
		}
		// Context is closed. Shutdown
		os.Exit(0)
	}()

	// Setup scheme for all stork resources
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Setup scheme failed for stork resources: %v", err)
	}

	var d volume.Driver
	if driverName != "" {
		log.Infof("Using driver %v", driverName)
		d, err = volume.Get(driverName)
		if err != nil {
			log.Fatalf("Error getting Stork Driver %v: %v", driverName, err)
		}

		if err = d.Init(nil); err != nil {
			log.Fatalf("Error initializing Stork Driver %v: %v", driverName, err)
		}

		if c.Bool("enable-metrics") {
			http.Handle("/metrics", promhttp.Handler())
			enableAppController := false
			enableMigrController := false
			if c.Bool("application-controller") {
				enableAppController = true
			}
			if c.Bool("migration-controller") {
				enableMigrController = true
			}
			if err = metrics.StartMetrics(enableAppController, enableMigrController); err != nil {
				log.Fatalf("Error starting prometheus metrics for stork: %v", err)
			}
		}

		if c.Bool("extender") {
			ext = &extender.Extender{
				Driver:   d,
				Recorder: recorder,
			}

			if err = ext.Start(); err != nil {
				log.Fatalf("Error starting scheduler extender: %v", err)
			}
		}
		if c.Bool("webhook-controller") {
			webhook = &webhookadmission.Controller{
				Driver:       d,
				Recorder:     recorder,
				SkipResource: c.String("webhook-skip-resources-annotation"),
			}
			if err := webhook.Start(); err != nil {
				log.Fatalf("error starting webhook controller: %v", err)
			}
		}
	}

	runFunc := func(context.Context) {
		runStork(mgr, mgrCtx, d, recorder, c, qps, burst)
	}

	if c.BoolT("leader-elect") {
		lockObjectName := c.String("lock-object-name")
		lockObjectNamespace := c.String("lock-object-namespace")
		id, err := os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %v", err)
		}
		lockConfig := resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		}
		resourceLock, err := resourcelock.New(
			resourcelock.ConfigMapsLeasesResourceLock,
			lockObjectNamespace,
			lockObjectName,
			k8sClient.CoreV1(),
			k8sClient.CoordinationV1(),
			lockConfig)
		if err != nil {
			log.Fatalf("Error creating resource lock: %v", err)
		}
		leaderElectionConfig := leaderelection.LeaderElectionConfig{
			Lock:          resourceLock,
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: runFunc,
				OnStoppedLeading: func() {
					log.Fatalf("Stork lost master")
				},
				OnNewLeader: displayLeader,
			},
		}
		leaderElector, err := leaderelection.NewLeaderElector(leaderElectionConfig)
		if err != nil {
			log.Fatalf("Error creating leader elector: %v", err)
		}
		leaderElector.Run(context.Background())
	} else {
		runFunc(nil)
	}
}

func displayLeader(name string) {
	log.Infof("new leader detected, current leader: %s", name)
}

func runStork(mgr manager.Manager, ctx context.Context, d volume.Driver, recorder record.EventRecorder, c *cli.Context, qps float32, burst int) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	if err := rule.Init(); err != nil {
		log.Fatalf("Error initializing rule: %v", err)
	}

	resourceCollector := resourcecollector.ResourceCollector{
		Driver: d,
		QPS:    qps,
		Burst:  burst,
	}
	if err := resourceCollector.Init(nil); err != nil {
		log.Fatalf("Error initializing ResourceCollector: %v", err)
	}
	adminNamespace := c.String("admin-namespace")
	if adminNamespace == "" {
		adminNamespace = c.String("migration-admin-namespace")
	}

	// Setting up the pluralmap. It has the right plural for a crd kind installed in the cluster.
	if err := pluralmap.CreateCRDPlurals(); err != nil {
		log.Fatalf("failed to setup crd plural map: %v", err)
	}
	log.Infof("crd plural map has been intialized")

	monitor := &monitor.Monitor{
		Driver:      d,
		IntervalSec: c.Int64("health-monitor-interval"),
		Recorder:    recorder,
	}
	snapshot := &snapshot.Snapshot{
		Driver:   d,
		Recorder: recorder,
	}
	if err := schedule.Init(); err != nil {
		log.Fatalf("Error initializing schedule: %v", err)
	}
	if d != nil {
		if c.Bool("health-monitor") {
			if err := monitor.Start(); err != nil {
				log.Fatalf("Error starting storage monitor: %v", err)
			}
		}

		if c.Bool("snapshotter") {
			if err := snapshot.Start(mgr); err != nil {
				log.Fatalf("Error starting snapshot controller: %v", err)
			}

			groupsnapshotInst := groupsnapshot.GroupSnapshot{
				Driver:   d,
				Recorder: recorder,
			}
			if err := groupsnapshotInst.Init(mgr); err != nil {
				log.Fatalf("Error initializing groupsnapshot controller: %v", err)
			}
		}
		if c.Bool("pvc-watcher") {
			pvcWatcher := pvcwatcher.New(mgr, d, recorder)
			if err := pvcWatcher.Start(mgr); err != nil {
				log.Fatalf("Error starting pvc watcher: %v", err)
			}
		}

		if c.Bool("migration-controller") {
			migration := migration.Migration{
				Driver:            d,
				Recorder:          recorder,
				ResourceCollector: resourceCollector,
			}
			if err := migration.Init(mgr, adminNamespace, c.Int("migration-max-threads")); err != nil {
				log.Fatalf("Error initializing migration: %v", err)
			}
		}

		if c.Bool(controllers.ResourceTransformationControllerName) {
			rt := controllers.NewResourceTransformation(mgr, d, recorder, resourceCollector)
			if err := rt.Init(mgr); err != nil {
				log.Fatalf("Error initializing resource transformation controller: %v", err)
			}
		}

		if c.Bool("cluster-domain-controllers") {
			clusterDomains := clusterdomains.ClusterDomains{
				Driver:   d,
				Recorder: recorder,
			}
			if err := clusterDomains.Init(mgr); err != nil {
				log.Fatalf("Error initializing cluster domain controllers: %v", err)
			}
		}
	}

	if c.Bool("application-controller") {
		appManager := applicationmanager.ApplicationManager{
			Driver:            d,
			Recorder:          recorder,
			ResourceCollector: resourceCollector,
			RsyncTime:         c.Int64("application-backup-sync-interval"),
		}
		if err := appManager.Init(mgr, adminNamespace, signalChan); err != nil {
			log.Fatalf("Error initializing application manager: %v", err)
		}
	}
	if c.Bool("px-object-controller") {
		objectController := &objectcontroller.ObjectController{}
		if err := objectController.Init(); err != nil {
			log.Warnf("Error initializing px-object-controller : %v", err)
		}
	}
	if c.Bool("kdmp-controller") {
		// Setup scheme for controllers resources
		if err := kdmpapi.AddToScheme(mgr.GetScheme()); err != nil {
			log.Fatalf("Setup scheme for kdmp resources: %v", err)
		}
		dataexport, err := dataexport.NewController(mgr)
		if err != nil {
			log.Fatalf("Error initializing kdmp controller: %v", err)
		}
		if err := dataexport.Init(mgr); err != nil {
			log.Fatalf("Error initializing kdmp controller: %v", err)
		}
		resourceexport, err := resourceexport.NewController(mgr)
		if err != nil {
			log.Fatalf("Error initializing resource export controller: %v", err)
		}

		if err := resourceexport.Init(mgr); err != nil {
			log.Fatalf("Error initializing resource export controller manager: %v", err)
		}
	}

	// Setup stork cache. We setup this cache for all the stork pods instead of just the leader pod.
	// In this way, even the stork extender code can use this cache, since the extender filter/process
	// requests can land on any stork pod.
	if err := cache.CreateSharedInformerCache(mgr); err != nil {
		log.Fatalf("failed to setup shared informer cache: %v", err)
	}
	log.Infof("shared informer cache has been intialized")

	go func() {
		for {
			<-signalChan
			log.Printf("Shutdown signal received, exiting...")
			if c.Bool("extender") {
				if err := ext.Stop(); err != nil {
					log.Warnf("Error stopping extender: %v", err)
				}
			}
			if c.Bool("health-monitor") {
				if err := monitor.Stop(); err != nil {
					log.Warnf("Error stopping monitor: %v", err)
				}
			}
			if c.Bool("snapshotter") {
				if err := snapshot.Stop(); err != nil {
					log.Warnf("Error stopping snapshot controllers: %v", err)
				}
			}
			if err := d.Stop(); err != nil {
				log.Warnf("Error stopping driver: %v", err)
			}
			if c.Bool("webhook-controller") {
				if err := webhook.Stop(); err != nil {
					log.Warnf("error stopping webhook controller %v", err)
				}
			}
			ctx.Done()
		}
	}()
}
