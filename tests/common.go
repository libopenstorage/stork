package tests

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/sirupsen/logrus"
	// import aws driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/aws"
	// import ssh driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	// import scheduler drivers to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/dcos"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	// import portworx driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	// defaultSpecsRoot specifies the default location of the base specs directory in the Torpedo container
	defaultSpecsRoot                   = "/specs"
	schedulerCliFlag                   = "scheduler"
	nodeDriverCliFlag                  = "node-driver"
	storageDriverCliFlag               = "storage-driver"
	specDirCliFlag                     = "spec-dir"
	appListCliFlag                     = "app-list"
	logLocationCliFlag                 = "log-location"
	scaleFactorCliFlag                 = "scale-factor"
	storageDriverUpgradeVersionCliFlag = "storage-driver-upgrade-version"
	storageDriverBaseVersionCliFlag    = "storage-driver-base-version"
)

const (
	defaultScheduler      = "k8s"
	defaultNodeDriver     = "ssh"
	defaultStorageDriver  = "pxd"
	defaultLogLocation    = "/mnt/torpedo_support_dir"
	defaultAppScaleFactor = 1
	// TODO: These are Portworx specific versions and will not work with other storage drivers.
	// Eventually we should remove the defaults and make it mandatory with documentation.
	defaultStorageDriverUpgradeVersion = "1.2.11.6"
	defaultStorageDriverBaseVersion    = "1.2.11.5"
)

const (
	waitResourceCleanup = 2 * time.Minute
)

var (
	context = ginkgo.Context
	// Step is an alias for ginko "By" which represents a step in the spec
	Step         = ginkgo.By
	expect       = gomega.Expect
	haveOccurred = gomega.HaveOccurred
	beEmpty      = gomega.BeEmpty
)

// InitInstance is the ginkgo spec for initializing torpedo
func InitInstance() {
	var err error
	err = Inst().S.Init(Inst().SpecDir, Inst().V.String(), Inst().N.String())
	expect(err).NotTo(haveOccurred())

	err = Inst().V.Init(Inst().S.String(), Inst().N.String())
	expect(err).NotTo(haveOccurred())

	err = Inst().N.Init()
	expect(err).NotTo(haveOccurred())
}

// ValidateCleanup checks that there are no resource leaks after the test run
func ValidateCleanup() {
	Step(fmt.Sprintf("validate cleanup of resources used by the test suite"), func() {
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ValidateVolumeCleanup(); err != nil {
				return "", true, err
			}

			return "", false, nil
		}

		_, err := task.DoRetryWithTimeout(t, waitResourceCleanup, 10*time.Second)
		expect(err).NotTo(haveOccurred())
	})
}

// ValidateContext is the ginkgo spec for validating a scheduled context
func ValidateContext(ctx *scheduler.Context) {
	ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		Step(fmt.Sprintf("validate %s app's volumes", ctx.App.Key), func() {
			ValidateVolumes(ctx)
		})

		Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
			err := Inst().S.WaitForRunning(ctx)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key), func() {
			vols, err := Inst().S.GetVolumes(ctx)
			expect(err).NotTo(haveOccurred())

			for _, vol := range vols {
				Step(fmt.Sprintf("validate if %s app's volume: %v is setup", ctx.App.Key, vol), func() {
					err := Inst().V.ValidateVolumeSetup(vol)
					expect(err).NotTo(haveOccurred())
				})
			}
		})
	})
}

// ValidateVolumes is the ginkgo spec for validating volumes of a context
func ValidateVolumes(ctx *scheduler.Context) {
	context("For validation of an app's volumes", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			err = Inst().S.InspectVolumes(ctx)
			expect(err).NotTo(haveOccurred())
		})

		var vols map[string]map[string]string
		Step(fmt.Sprintf("get %s app's volume's custom parameters", ctx.App.Key), func() {
			vols, err = Inst().S.GetVolumeParameters(ctx)
			expect(err).NotTo(haveOccurred())
		})

		for vol, params := range vols {
			Step(fmt.Sprintf("get %s app's volume: %s inspected by the volume driver", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateVolume(vol, params)
				expect(err).NotTo(haveOccurred())
			})
		}
	})
}

// TearDownContext is the ginkgo spec for tearing down a scheduled context
func TearDownContext(ctx *scheduler.Context, opts map[string]bool) {
	context("For tearing down of an app context", func() {
		var err error

		Step(fmt.Sprintf("start destroying %s app", ctx.App.Key), func() {
			err = Inst().S.Destroy(ctx, opts)
			expect(err).NotTo(haveOccurred())
		})

		DeleteVolumesAndWait(ctx)
	})
}

// DeleteVolumesAndWait deletes volumes of given context and waits till they are deleted
func DeleteVolumesAndWait(ctx *scheduler.Context) {
	var err error
	var vols []*volume.Volume
	Step(fmt.Sprintf("destroy the %s app's volumes", ctx.App.Key), func() {
		vols, err = Inst().S.DeleteVolumes(ctx)
		expect(err).NotTo(haveOccurred())
	})

	for _, vol := range vols {
		Step(fmt.Sprintf("validate %s app's volume %s has been deleted in the volume driver",
			ctx.App.Key, vol.Name), func() {
			err = Inst().V.ValidateDeleteVolume(vol)
			expect(err).NotTo(haveOccurred())
		})
	}
}

// ScheduleAndValidate schedules and validates applications
func ScheduleAndValidate(testname string) []*scheduler.Context {
	var contexts []*scheduler.Context
	var err error

	Step("schedule applications", func() {
		taskName := fmt.Sprintf("%s-%v", testname, Inst().InstanceID)
		contexts, err = Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
			AppKeys: Inst().AppList,
		})
		expect(err).NotTo(haveOccurred())
		expect(contexts).NotTo(beEmpty())
	})

	Step("validate applications", func() {
		for _, ctx := range contexts {
			ValidateContext(ctx)
		}
	})

	return contexts
}

// StartVolDriverAndWait starts volume driver on given app nodes
func StartVolDriverAndWait(appNodes []node.Node) {
	context(fmt.Sprintf("starting volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("start volume driver on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.StartDriver(n)
				expect(err).NotTo(haveOccurred())
			}
		})

		Step(fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverUpOnNode(n)
				expect(err).NotTo(haveOccurred())
			}
		})

	})
}

// StopVolDriverAndWait stops volume driver on given app nodes and waits till driver is down
func StopVolDriverAndWait(appNodes []node.Node) {
	context(fmt.Sprintf("stopping volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("stop volume driver on nodes: %v", appNodes), func() {
			err := Inst().V.StopDriver(appNodes, false)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("wait for volume driver to stop on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverDownOnNode(n)
				expect(err).NotTo(haveOccurred())
			}
		})

	})
}

// CrashVolDriverAndWait crashes volume driver on given app nodes and waits till driver is back up
func CrashVolDriverAndWait(appNodes []node.Node) {
	context(fmt.Sprintf("crashing volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("crash volume driver on nodes: %v", appNodes), func() {
			err := Inst().V.StopDriver(appNodes, true)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverUpOnNode(n)
				expect(err).NotTo(haveOccurred())
			}
		})

	})
}

// ValidateAndDestroy validates application and then destroys them
func ValidateAndDestroy(contexts []*scheduler.Context, opts map[string]bool) {
	Step("validate apps", func() {
		for _, ctx := range contexts {
			ValidateContext(ctx)
		}
	})

	Step("destroy apps", func() {
		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
}

// CollectSupport creates a support bundle
func CollectSupport() {
	context(fmt.Sprintf("generating support bundle..."), func() {
		Step(fmt.Sprintf("save journal output on each node"), func() {
			nodes := node.GetWorkerNodes()
			expect(nodes).NotTo(beEmpty())

			journalCmd := fmt.Sprintf(
				"sudo su -c 'echo t > /proc/sysrq-trigger && journalctl -l > ~/all_journal_%v'",
				time.Now().Format(time.RFC3339))
			for _, n := range nodes {
				_, err := Inst().N.RunCommand(n, journalCmd, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				if err != nil {
					logrus.Warnf("failed to run cmd: %s. err: %v", journalCmd, err)
				}
			}
		})
	})
}

// Inst returns the Torpedo instances
func Inst() *Torpedo {
	return instance
}

var instance *Torpedo
var once sync.Once

// Torpedo is the torpedo testsuite
type Torpedo struct {
	InstanceID                  string
	S                           scheduler.Driver
	V                           volume.Driver
	N                           node.Driver
	SpecDir                     string
	AppList                     []string
	LogLoc                      string
	ScaleFactor                 int
	StorageDriverUpgradeVersion string
	StorageDriverBaseVersion    string
}

// ParseFlags parses command line flags
func ParseFlags() {
	var err error
	var s, n, v, specDir, logLoc, appListCSV string
	var schedulerDriver scheduler.Driver
	var volumeDriver volume.Driver
	var nodeDriver node.Driver
	var appScaleFactor int
	var volUpgradeVersion, volBaseVersion string

	flag.StringVar(&s, schedulerCliFlag, defaultScheduler, "Name of the scheduler to us")
	flag.StringVar(&n, nodeDriverCliFlag, defaultNodeDriver, "Name of the node driver to use")
	flag.StringVar(&v, storageDriverCliFlag, defaultStorageDriver, "Name of the storage driver to use")
	flag.StringVar(&specDir, specDirCliFlag, defaultSpecsRoot, "Root directory containing the application spec files")
	flag.StringVar(&logLoc, logLocationCliFlag, defaultLogLocation,
		"Path to save logs/artifacts upon failure. Default: /mnt/torpedo_support_dir")
	flag.IntVar(&appScaleFactor, scaleFactorCliFlag, defaultAppScaleFactor, "Factor by which to scale applications")
	flag.StringVar(&volUpgradeVersion, storageDriverUpgradeVersionCliFlag, defaultStorageDriverUpgradeVersion,
		"Version of storage driver to be upgraded to")
	flag.StringVar(&volBaseVersion, storageDriverBaseVersionCliFlag, defaultStorageDriverBaseVersion,
		"Version of storage driver to be downgraded to")
	flag.StringVar(&appListCSV, appListCliFlag, "", "Comma-separated list of apps to run as part of test. The names should match directories in the spec dir.")

	flag.Parse()

	appList, err := splitCsv(appListCSV)
	if err != nil {
		logrus.Fatalf("failed to parse app list: %v. err: %v", appListCSV, err)
	}

	if schedulerDriver, err = scheduler.Get(s); err != nil {
		logrus.Fatalf("Cannot find scheduler driver for %v. Err: %v\n", s, err)
	} else if volumeDriver, err = volume.Get(v); err != nil {
		logrus.Fatalf("Cannot find volume driver for %v. Err: %v\n", v, err)
	} else if nodeDriver, err = node.Get(n); err != nil {
		logrus.Fatalf("Cannot find node driver for %v. Err: %v\n", n, err)
	} else if err := os.MkdirAll(logLoc, os.ModeDir); err != nil {
		logrus.Fatalf("Cannot create path %s for saving support bundle. Error: %v", logLoc, err)
	} else {
		once.Do(func() {
			instance = &Torpedo{
				InstanceID:                  time.Now().Format("01-02-15h04m05s"),
				S:                           schedulerDriver,
				V:                           volumeDriver,
				N:                           nodeDriver,
				SpecDir:                     specDir,
				LogLoc:                      logLoc,
				ScaleFactor:                 appScaleFactor,
				StorageDriverUpgradeVersion: volUpgradeVersion,
				StorageDriverBaseVersion:    volBaseVersion,
				AppList:                     appList,
			}
		})
	}
}

func splitCsv(in string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(in))
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil || len(records) < 1 {
		return []string{}, err
	} else if len(records) > 1 {
		return []string{}, fmt.Errorf("Multiline CSV not supported")
	}
	return records[0], err
}

func init() {
	logrus.SetLevel(logrus.InfoLevel)
	logrus.StandardLogger().Hooks.Add(log.NewHook())
	logrus.SetOutput(ginkgo.GinkgoWriter)
}
