package tests

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	// import aws driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/aws"
	// import ssh driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	// import k8s driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	// import portworx driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/util"
)

const (
	// defaultSpecsRoot specifies the default location of the base specs directory in the Torpedo container
	defaultSpecsRoot     = "/specs"
	schedulerCliFlag     = "scheduler"
	nodeDriverCliFlag    = "node-driver"
	storageDriverCliFlag = "storage-driver"
	specDirCliFlag       = "spec-dir"
	logLocationCliFlag   = "log-location"
)

const (
	defaultScheduler     = "k8s"
	defaultNodeDriver    = "ssh"
	defaultStorageDriver = "pxd"
	defaultLogLocation   = "/mnt/torpedo_support_dir"
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
	err = Inst().S.Init(Inst().SpecDir, Inst().N.String())
	expect(err).NotTo(haveOccurred())

	err = Inst().V.Init(Inst().S.String())
	expect(err).NotTo(haveOccurred())

	err = Inst().N.Init(Inst().S.String())
	expect(err).NotTo(haveOccurred())
}

// ValidateContext is the ginkgo spec for validating a scheduled context
func ValidateContext(ctx *scheduler.Context) {
	ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		generateSupportBundle(ctx)

		Step(fmt.Sprintf("validate %s app's volumes", ctx.App.Key), func() {
			ValidateVolumes(ctx)
		})

		Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
			err := Inst().S.WaitForRunning(ctx)
			expect(err).NotTo(haveOccurred())
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
			Step(fmt.Sprintf("get %s app's volume inspected by the volume driver", ctx.App.Key), func() {
				err = Inst().V.InspectVolume(vol, params)
				expect(err).NotTo(haveOccurred())
			})
		}
	})
}

// TearDownContext is the ginkgo spec for tearing down a scheduled context
func TearDownContext(ctx *scheduler.Context, opts map[string]bool) {
	context("For tearing down of an app context", func() {
		Step(fmt.Sprintf("start destroying %s app", ctx.App.Key), func() {
			err := Inst().S.Destroy(ctx, opts)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("wait for %s app to terminate", ctx.App.Key), func() {
			err := Inst().S.WaitForDestroy(ctx)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("destroy the %s app's volumes", ctx.App.Key), func() {
			err := Inst().S.DeleteVolumes(ctx)
			expect(err).NotTo(haveOccurred())
		})
	})
}

// ScheduleAndValidate schedules and validates applications
func ScheduleAndValidate(testname string) []*scheduler.Context {
	var contexts []*scheduler.Context
	var err error

	Step("schedule applications", func() {
		taskName := fmt.Sprintf("%s-%v", testname, Inst().InstanceID)
		contexts, err = Inst().S.Schedule(taskName, scheduler.ScheduleOptions{})
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
				err := Inst().V.WaitStart(n)
				expect(err).NotTo(haveOccurred())
			}
		})

	})
}

// ValidateAndDestroy validates application and then destroys them
func ValidateAndDestroy(ctx *scheduler.Context, opts map[string]bool) {
	ValidateContext(ctx)
	TearDownContext(ctx, opts)
}

// generateSupportBundle gathers logs and any artifacts pertinent to the scheduler and dumps them in the defined location
func generateSupportBundle(ctx *scheduler.Context) {
	context(fmt.Sprintf("generate support bundle for app: %s", ctx.App.Key), func() {
		var out string
		var err error

		Step(fmt.Sprintf("describe scheduler context for app: %s", ctx.App.Key), func() {
			out, err = Inst().S.Describe(ctx)
			expect(err).NotTo(haveOccurred())

			err = ioutil.WriteFile(fmt.Sprintf("%s/supportbundle_%s_%v.log",
				Inst().LogLoc, ctx.UID, time.Now().Format(time.RFC3339)), []byte(out), 0644)
			expect(err).NotTo(haveOccurred())
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
	InstanceID string
	S          scheduler.Driver
	V          volume.Driver
	N          node.Driver
	SpecDir    string
	LogLoc     string
}

// ParseFlags parses command line flags
func ParseFlags() {
	var err error
	var s, n, v, specDir, logLoc string
	var schedulerDriver scheduler.Driver
	var volumeDriver volume.Driver
	var nodeDriver node.Driver

	flag.StringVar(&s, schedulerCliFlag, defaultScheduler, "Name of the scheduler to us")
	flag.StringVar(&n, nodeDriverCliFlag, defaultNodeDriver, "Name of the node driver to use")
	flag.StringVar(&v, storageDriverCliFlag, defaultStorageDriver, "Name of the storage driver to use")
	flag.StringVar(&specDir, specDirCliFlag, defaultSpecsRoot,
		"Root directory containing the application spec files")
	flag.StringVar(&logLoc, logLocationCliFlag, defaultLogLocation,
		"Path to save logs/artifacts upon failure. Default: /mnt/torpedo_support_dir")

	flag.Parse()

	if schedulerDriver, err = scheduler.Get(s); err != nil {
		util.Fatalf("Cannot find scheduler driver for %v. Err: %v\n", s, err)
		os.Exit(-1)
	} else if volumeDriver, err = volume.Get(v); err != nil {
		util.Fatalf("Cannot find volume driver for %v. Err: %v\n", v, err)
		os.Exit(-1)
	} else if nodeDriver, err = node.Get(n); err != nil {
		util.Fatalf("Cannot find node driver for %v. Err: %v\n", n, err)
		os.Exit(-1)
	} else if err := os.MkdirAll(logLoc, os.ModeDir); err != nil {
		util.Fatalf("Cannot create path %s for saving support bundle. Error: %v", logLoc, err)
		os.Exit(-1)
	} else {
		once.Do(func() {
			instance = &Torpedo{
				InstanceID: time.Now().Format("01-02-15h04m05s"),
				S:          schedulerDriver,
				V:          volumeDriver,
				N:          nodeDriver,
				SpecDir:    specDir,
				LogLoc:     logLoc,
			}
		})
	}
}
