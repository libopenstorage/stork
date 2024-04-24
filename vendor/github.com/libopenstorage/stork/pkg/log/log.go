package log

import (
	"crypto/tls"
	"fmt"

	"io"
	defaultLogger "log"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	aetosutil "github.com/libopenstorage/stork/pkg/aetosutils"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/sirupsen/logrus"
	appv1 "k8s.io/api/apps/v1"
	appv1beta1 "k8s.io/api/apps/v1beta1"
	appv1beta2 "k8s.io/api/apps/v1beta2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	Dash            = aetosutil.Get()
	log             *logrus.Logger
	lock            = &sync.Mutex{}
	storkLogFile    *os.File
	enableDashboard = true
)

const (
	NfsLogServer = "http://aetos-nfs-01.pwx.purestorage.com/"
)

// CreateLogFile creates file and return the file object
func CreateLogFile(filename string) *os.File {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("Failed to create logfile %s", filename)
		log.Errorf("Error: %v", err)
	}
	return f
}

// SetStorkTestFileOutput adds output destination for logging
func SetStorkTestFileOutput(log *logrus.Logger, f *os.File) {
	log.Out = io.MultiWriter(log.Out, f)
	log.Debugf("Log Dir: %s", f.Name())
}

func IsDashboardReachable() bool {
	timeout := 5 * time.Second
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := http.Client{
		Transport: tr,
		Timeout:   timeout,
	}
	if _, err := client.Get(aetosutil.AetosUrl); err != nil {
		log.Warn("dashboard is not reachable", err)
		return false
	}
	return true
}
func GenerateNFSUrl(inputPath string) string {
	return strings.ReplaceAll(inputPath, "/pwx/aetos/", NfsLogServer)
}

func dirExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func GetLogDir(testLogDir string) string {
	var err error
	if testLogDir != "." {
		if !dirExists(testLogDir) {
			err = os.Mkdir(testLogDir, os.ModePerm)
			if err != nil {
				log.Error(err)
				return "."
			}
		}
	}
	Dash.LogDir = testLogDir
	storkLogPath := fmt.Sprintf("%s/%s", Dash.LogDir, "test.log")
	logUrl := GenerateNFSUrl(storkLogPath)
	Dash.LogUrl = logUrl
	return storkLogPath
}

func SetLoglevel(testLog *logrus.Logger, logLevel string) {
	switch logLevel {
	case "debug":
		testLog.Level = logrus.DebugLevel
	case "info":
		testLog.Level = logrus.InfoLevel
	case "error":
		testLog.Level = logrus.ErrorLevel
	case "warn":
		testLog.Level = logrus.WarnLevel
	default:
		testLog.Level = logrus.DebugLevel

	}
}

func verifyDashboard() {
	if Dash == nil {
		aeosLogs := GetLogInstance()
		Dash.TestLog = aeosLogs
		aeosLogs.Out = io.MultiWriter(aeosLogs.Out)
		SetLoglevel(aeosLogs, "debug")
		storkLogPath := GetLogDir(".")
		fmt.Printf("inside verifyDashboard testLogPath is %s \n", storkLogPath)
		storkLogFile = CreateLogFile(storkLogPath)
		if storkLogFile != nil {
			SetStorkTestFileOutput(aeosLogs, storkLogFile)
		}
		if enableDashboard && !IsDashboardReachable() {
			enableDashboard = false
			log.Warn("Aetos Dashboard is not reachable. Disabling dashboard reporting.")
		}
		fmt.Println("Dashboard in verifyDashboard is enabled: ", enableDashboard)
		Dash.IsEnabled = enableDashboard
	}
}

func Fatal(format string, args ...interface{}) {
	Dash.Fatal(format, args...)
	logrus.Fatalf(format, args...)
}

func FailOnError(t *testing.T, err error, description string, args ...interface{}) {
	if err != nil {
		Dash.VerifyFatal(t, "Error", "NoError", fmt.Sprintf("%v. Err: %v", fmt.Sprintf(description, args...), err))
	}
}

func FailOnNoError(t *testing.T, err error, description string, args ...interface{}) {
	if err == nil {
		Dash.VerifyFatal(t, "NoError", "Error", fmt.Sprintf("%v. Err: %v", fmt.Sprintf(description, args...), err))
	}
}

func Error(format string, args ...interface{}) {
	verifyDashboard()
	Dash.Errorf(format, args...)
	Dash.TestLog.Errorf(format, args...)
	defaultLogger.Printf("ERROR: %s", fmt.Sprintf(format, args...))
}

func Debug(msg string) {
	Dash.TestLog.Debug(msg)
	defaultLogger.Printf("DEBUG: %s", msg)
}

func Debugf(format string, args ...interface{}) {
	Dash.TestLog.Debugf(format, args...)
	defaultLogger.Printf("DEBUG: %s", fmt.Sprintf(format, args...))
}

func Warn(format string, args ...interface{}) {
	verifyDashboard()
	Dash.TestLog.Warnf(format, args...)
	Dash.Warnf(format, args...)
	defaultLogger.Printf("WARN: %s", fmt.Sprintf(format, args...))
}

func Info(format string, args ...interface{}) {
	Dash.TestLog.Infof(format, args...)
	defaultLogger.Printf("INFO: %s", fmt.Sprintf(format, args...))
}

func InfoD(format string, args ...interface{}) {
	verifyDashboard()
	Dash.TestLog.Infof(format, args...)
	Dash.Infof(format, args...)
	defaultLogger.Printf("INFO: %s", fmt.Sprintf(format, args...))
}

// GetLogInstance returns the logrus instance
func GetLogInstance() *logrus.Logger {

	//To-DO: add rolling file appender
	//max: 50MB and 10 files

	if log == nil {
		lock.Lock()
		defer lock.Unlock()
		if log == nil {
			log = logrus.New()
			log.Out = io.MultiWriter(os.Stdout)
		}
	}
	return log
}

// PodLog Format a log message with pod information
func PodLog(pod *v1.Pod) *logrus.Entry {
	if pod != nil {
		fields := logrus.Fields{
			"PodName":   pod.Name,
			"Namespace": pod.Namespace,
		}
		for _, owner := range pod.OwnerReferences {
			if owner.Controller != nil && *owner.Controller {
				fields["Owner"] = owner.Kind + "/" + owner.Name
				break
			}
		}
		return logrus.WithFields(fields)
	}
	return logrus.WithFields(logrus.Fields{})
}

// DeploymentV1Log Format a log message with deployment information
func DeploymentV1Log(deployment *appv1.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// DeploymentV1Beta1Log Format a log message with deployment information
func DeploymentV1Beta1Log(deployment *appv1beta1.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// DeploymentV1Beta2Log Format a log message with deployment information
func DeploymentV1Beta2Log(deployment *appv1beta2.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// StatefulSetV1Log Format a log message with statefulset information
func StatefulSetV1Log(ss *appv1.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// StatefulSetV1Beta1Log Format a log message with statefulset information
func StatefulSetV1Beta1Log(ss *appv1beta1.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// StatefulSetV1Beta2Log Format a log message with statefulset information
func StatefulSetV1Beta2Log(ss *appv1beta2.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// SnapshotLog formats a log message with snapshot information
func SnapshotLog(snap *crdv1.VolumeSnapshot) *logrus.Entry {
	if snap != nil {
		return logrus.WithFields(logrus.Fields{
			"VolumeSnapshotName": snap.Metadata.Name,
			"Namespace":          snap.Metadata.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// VolumeSnapshotScheduleLog formats a log message with snapshotschedule information
func VolumeSnapshotScheduleLog(snapshotSchedule *storkv1.VolumeSnapshotSchedule) *logrus.Entry {
	if snapshotSchedule != nil {
		return logrus.WithFields(logrus.Fields{
			"VolumeSnapshotScheduleName": snapshotSchedule.Name,
			"Namespace":                  snapshotSchedule.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// RuleLog formats a log message with Rule information
func RuleLog(
	rule *storkv1.Rule,
	object runtime.Object,
) *logrus.Entry {
	fields := logrus.Fields{}
	metadata, err := meta.Accessor(object)
	if err == nil {
		fields["Name"] = metadata.GetName()
		namespace := metadata.GetNamespace()
		if namespace != "" {
			fields["Namespace"] = namespace
		}
	}
	objectType, err := meta.TypeAccessor(object)
	if err == nil {
		fields["Kind"] = objectType.GetKind()
	}
	if rule != nil {
		fields["Rule"] = rule.Name
	}
	return logrus.WithFields(fields)

}

// MigrationLog formats a log message with migration information
func MigrationLog(migration *storkv1.Migration) *logrus.Entry {
	if migration != nil {
		return logrus.WithFields(logrus.Fields{
			"MigrationName": migration.Name,
			"Namespace":     migration.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// TransformLog formats a log message with resource transformation CR information
func TransformLog(transform *storkv1.ResourceTransformation) *logrus.Entry {
	if transform != nil {
		return logrus.WithFields(logrus.Fields{
			"ResourceTransformationName":      transform.Name,
			"ResourceTransformationNamespace": transform.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// MigrationScheduleLog formats a log message with migrationschedule information
func MigrationScheduleLog(migrationSchedule *storkv1.MigrationSchedule) *logrus.Entry {
	if migrationSchedule != nil {
		return logrus.WithFields(logrus.Fields{
			"MigrationScheduleName": migrationSchedule.Name,
			"Namespace":             migrationSchedule.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// GroupSnapshotLog formats a log message with groupsnapshot information
func GroupSnapshotLog(groupsnapshot *storkv1.GroupVolumeSnapshot) *logrus.Entry {
	if groupsnapshot != nil {
		return logrus.WithFields(logrus.Fields{
			"GroupSnapshotName": groupsnapshot.Name,
			"Namespace":         groupsnapshot.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// ClusterDomainUpdateLog formats a log message with clusterdomainupdate information
func ClusterDomainUpdateLog(clusterDomainUpdate *storkv1.ClusterDomainUpdate) *logrus.Entry {
	if clusterDomainUpdate != nil {
		return logrus.WithFields(logrus.Fields{
			"ClusterDomainName": clusterDomainUpdate.Spec.ClusterDomain,
			"Active":            clusterDomainUpdate.Spec.Active,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// PVCLog formats a log message with pvc information
func PVCLog(pvc *v1.PersistentVolumeClaim) *logrus.Entry {
	if pvc != nil {
		return logrus.WithFields(logrus.Fields{
			"PVCName":   pvc.Name,
			"Namespace": pvc.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// PVLog formats a log message with pv information
func PVLog(pv *v1.PersistentVolume) *logrus.Entry {
	if pv != nil {
		return logrus.WithFields(logrus.Fields{
			"PVName":    pv.Name,
			"Namespace": pv.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// ApplicationBackupLog formats a log message with applicationbackup information
func ApplicationBackupLog(backup *storkv1.ApplicationBackup) *logrus.Entry {
	if backup != nil {
		return logrus.WithFields(logrus.Fields{
			"ApplicationBackupName": backup.Name,
			"ApplicationBackupUID":  string(backup.UID),
			"Namespace":             backup.Namespace,
			"ResourceVersion":       backup.ResourceVersion,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// ApplicationRestoreLog formats a log message with applicationrestore information
func ApplicationRestoreLog(restore *storkv1.ApplicationRestore) *logrus.Entry {
	if restore != nil {
		return logrus.WithFields(logrus.Fields{
			"ApplicationRestoreName": restore.Name,
			"ApplicationRestoreUID":  string(restore.UID),
			"Namespace":              restore.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// ApplicationCloneLog formats a log message with applicationclone information
func ApplicationCloneLog(clone *storkv1.ApplicationClone) *logrus.Entry {
	if clone != nil {
		return logrus.WithFields(logrus.Fields{
			"ApplicationCloneName": clone.Name,
			"Namespace":            clone.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// VolumeSnapshotRestoreLog formats a log message with volumesnapshotrestore information
func VolumeSnapshotRestoreLog(snapRestore *storkv1.VolumeSnapshotRestore) *logrus.Entry {
	if snapRestore != nil {
		return logrus.WithFields(logrus.Fields{
			"VolumeSnapshotRestoreName": snapRestore.Name,
			"Namespace":                 snapRestore.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// ApplicationBackupScheduleLog formats a log message with applicationbackupschedule information
func ApplicationBackupScheduleLog(backupSchedule *storkv1.ApplicationBackupSchedule) *logrus.Entry {
	if backupSchedule != nil {
		return logrus.WithFields(logrus.Fields{
			"ApplicationBackupScheduleName": backupSchedule.Name,
			"Namespace":                     backupSchedule.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// BackupLocationLog formats a log message with backuplocation information
func BackupLocationLog(location *storkv1.BackupLocation) *logrus.Entry {
	if location != nil {
		return logrus.WithFields(logrus.Fields{
			"BackupLocationName": location.Name,
			"Namespace":          location.Namespace,
			"Type":               location.Location.Type,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// ActionLog formats a log message with action information
func ActionLog(action *storkv1.Action) *logrus.Entry {
	if action != nil {
		return logrus.WithFields(logrus.Fields{
			"ActionName": action.Name,
			"Namespace":  action.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}
