package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	dirName            = "./specs"
	bkpDirName         = "./backup"
	adminNamespace     = "kube-system"
	backuplocationName = "upgrade-bkp-location"
	upgradeSchedPolicy = "upgrade-test-sched-policy"
	bkpSchedName       = "upgrade-test-backup"
	migrSchedName      = "upgrade-test-migration"
	clusterPairName    = "upgrade-test-clusterpair"
	version            = "2.6-dev"
)

func main() {
	namespaces := []string{"upgrade-test"}
	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	logrus.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
	appOps, err := apps.NewInstanceFromConfigFile("/tmp/config")
	if err != nil {
		logrus.Errorf("unable to set app inst: %v", err)
		panic(err)
	}
	apps.SetInstance(appOps)
	storkOps, err := stork.NewInstanceFromConfigFile("/tmp/config")
	if err != nil {
		logrus.Errorf("unable to set app inst: %v", err)
		panic(err)
	}
	stork.SetInstance(storkOps)
	// create application in different namespaces
	if err := deployApps("/tmp/config", dirName, namespaces); err != nil {
		panic(err)
	}

	// Configure migration schedule
	// wait for migrations to trigger and validate
	migr, err := createMigrationSchedule("/tmp/config", bkpDirName, namespaces)
	if err != nil {
		panic(err)
	}
	logrus.Debugf("Migration before upgrade: %v", migr)
	bkp, err := createBackupSchedule("/tmp/config", bkpDirName, namespaces)
	if err != nil {
		panic(err)
	}
	logrus.Debugf("application backup before upgarde: %v", bkp)
	// upgrade stork version
	if err := upgradeStorkVersion(version); err != nil {
		panic(err)
	}
	// validate applicationbackups after upgrade
	if err := validateAppBackup(bkp); err != nil {
		panic(err)
	}
	// validate migrations after upgrade
	if err := validateMigrations(migr); err != nil {
		panic(err)
	}
	logrus.Infof("upgrade test done")
}

func deployApps(config, dirName string, namespaces []string) error {
	var outb, errb bytes.Buffer
	for _, namespace := range namespaces {
		cmd := exec.Command("kubectl", "--kubeconfig="+config, "create", "ns", namespace)
		cmd.Stdout = &outb
		cmd.Stderr = &errb
		err := cmd.Run()
		if err != nil {
			logrus.Errorf("unable to exec cmd: %v, err: %v\n", cmd, errb.String())
			return err
		}
		logrus.Infof("%s", outb.String())

		cmd = exec.Command("kubectl", "--kubeconfig="+config, "apply", "-f", dirName, "-n", namespace)
		cmd.Stdout = &outb
		cmd.Stderr = &errb
		err = cmd.Run()
		if err != nil {
			logrus.Errorf("unable to exec cmd: %v, err: %v\n", err.Error(), errb.String())
			return err
		}
		logrus.Infof("Creating apps:")
		logrus.Infof("%s", outb.String())
	}

	// wait for apps to in given namespaces
	for _, namespace := range namespaces {
		deployList, err := apps.Instance().ListDeployments(namespace, metav1.ListOptions{})
		if err != nil {
			logrus.Errorf("unable to list deploy: %v", err)
			return err
		}
		for _, deploy := range deployList.Items {
			if err := apps.Instance().ValidateDeployment(&deploy, 12*time.Minute, 10*time.Second); err != nil {
				return err
			}
		}
		// validate statefulset
		stsList, err := apps.Instance().ListStatefulSets(namespace)
		if err != nil {
			logrus.Errorf("unable to list deploy: %v", err)
			return err
		}
		for _, sts := range stsList.Items {
			if err := apps.Instance().ValidateStatefulSet(&sts, 5*time.Minute); err != nil {
				return err
			}
		}
	}

	return nil
}

func createBackupSchedule(config, backupDir string, namespaces []string) (*stork_api.ApplicationBackup, error) {
	logrus.Infof("create backup schedule")
	var outb, errb bytes.Buffer
	// create backuplocation
	cmd := exec.Command("kubectl", "--kubeconfig="+config, "apply", "-f",
		backupDir, "-n", adminNamespace)
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		logrus.Errorf("unable to exec cmd: %v, err: %v\n", cmd, errb.String())
		return nil, err
	}
	logrus.Infof("%s", outb.String())
	// create backupschedule
	bkpSched := &stork_api.ApplicationBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bkpSchedName,
			Namespace: adminNamespace,
		},
	}
	bkpSched.Spec.SchedulePolicyName = upgradeSchedPolicy
	bkpSched.Spec.Template.Spec = stork_api.ApplicationBackupSpec{
		Namespaces:     namespaces,
		BackupLocation: backuplocationName,
	}
	_, err = stork.Instance().CreateApplicationBackupSchedule(bkpSched)
	if err != nil {
		return nil, err
	}
	logrus.Infof("waiting for backups to be triggered... 15mins")
	// wait for at least 3 backups to success if any failed return err
	time.Sleep(15 * time.Minute)

	resp, err := stork.Instance().ValidateApplicationBackupSchedule(bkpSchedName, adminNamespace, 3, 15*time.Minute, 10*time.Second)
	if err != nil {
		return nil, err
	}

	// send last successful backup info back
	var orgBackup *stork_api.ApplicationBackup
	for _, v := range resp {
		orgBackup, err = stork.Instance().GetApplicationBackup(v[0].Name, adminNamespace)
		if err != nil {
			return nil, err
		}
		break
	}
	return orgBackup, err
}

func upgradeStorkVersion(version string) error {
	logrus.Infof("upgrading stork version to %s", version)
	deploy, err := apps.Instance().GetDeployment("stork", adminNamespace)
	if err != nil {
		return err
	}

	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("unable to update image: path not found")
	}
	logrus.Infof("current stork version: %s", deploy.Spec.Template.Spec.Containers[0].Image)
	deploy.Spec.Template.Spec.Containers[0].Image = "openstorage/stork:" + version
	resp, err := apps.Instance().UpdateDeployment(deploy)
	if err != nil {
		return err
	}
	if err := apps.Instance().ValidateDeployment(resp, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	logrus.Infof("updated stork version to %s", version)

	return nil
}

func validateAppBackup(orgBackup *stork_api.ApplicationBackup) error {
	logrus.Infof("validating app backups..waiting for 10 mins")

	time.Sleep(10 * time.Minute)

	// wait for at least 3 backups to success if any failed return err
	resp, err := stork.Instance().ValidateApplicationBackupSchedule(bkpSchedName, adminNamespace, 3, 15*time.Minute, 10*time.Second)
	if err != nil {
		return err
	}
	var upgradeBackup *stork_api.ApplicationBackup
	for _, v := range resp {
		upgradeBackup, err = stork.Instance().GetApplicationBackup(v[0].Name, adminNamespace)
		if err != nil {
			return err
		}

	}
	if len(orgBackup.Status.Volumes) != len(upgradeBackup.Status.Volumes) {
		logrus.Errorf("volumes before upgrade: %v", orgBackup.Status.Volumes)
		logrus.Errorf("volumes after upgrade: %v", upgradeBackup.Status.Volumes)
		return fmt.Errorf("volume counts after upgrade does not match, expected: %v, actual: %v", len(upgradeBackup.Status.Volumes), len(orgBackup.Status.Volumes))
	}
	if len(orgBackup.Status.Resources) != len(upgradeBackup.Status.Resources) {
		logrus.Errorf("volumes before upgrade: %v", orgBackup.Status.Resources)
		logrus.Errorf("volumes after upgrade: %v", upgradeBackup.Status.Resources)
		return fmt.Errorf("volume counts after upgrade does not match, expected: %v, actual: %v", len(upgradeBackup.Status.Resources), len(orgBackup.Status.Resources))
	}

	return nil
}

func createMigrationSchedule(config, backupDir string, namespaces []string) (*stork_api.Migration, error) {
	logrus.Infof("create migration schedule")
	// create migration
	migrSched := &stork_api.MigrationSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      migrSchedName,
			Namespace: adminNamespace,
		},
	}
	migrSched.Spec.SchedulePolicyName = upgradeSchedPolicy
	migrSched.Spec.Template.Spec = stork_api.MigrationSpec{
		Namespaces:  namespaces,
		ClusterPair: clusterPairName,
	}
	_, err := stork.Instance().CreateMigrationSchedule(migrSched)
	if err != nil {
		return nil, err
	}
	// wait for at least 3 backups to success if any failed return err
	logrus.Infof("waiting for migrations to start..15 min")
	time.Sleep(15 * time.Minute)

	resp, err := stork.Instance().ValidateMigrationSchedule(migrSchedName, adminNamespace, 15*time.Minute, 10*time.Second)
	if err != nil {
		return nil, err
	}

	// send last successful backup info back
	var oldMigr *stork_api.Migration
	for _, v := range resp {
		oldMigr, err = stork.Instance().GetMigration(v[0].Name, adminNamespace)
		if err != nil {
			return nil, err
		}
		break
	}
	return oldMigr, err
}

func validateMigrations(oldMigr *stork_api.Migration) error {
	logrus.Infof("validating migrations..wait 10 mins")

	time.Sleep(10 * time.Minute)

	// wait for at least 3 backups to success if any failed return err
	resp, err := stork.Instance().ValidateMigrationSchedule(migrSchedName, adminNamespace, 15*time.Minute, 10*time.Second)
	if err != nil {
		return err
	}

	var updatedMigr *stork_api.Migration
	for _, v := range resp {
		updatedMigr, err = stork.Instance().GetMigration(v[0].Name, adminNamespace)
		if err != nil {
			return err
		}

	}
	if len(oldMigr.Status.Volumes) != len(updatedMigr.Status.Volumes) {
		logrus.Errorf("volumes before upgrade: %v", oldMigr.Status.Volumes)
		logrus.Errorf("volumes after upgrade: %v", updatedMigr.Status.Volumes)
		return fmt.Errorf("volume counts after upgrade does not match, expected: %v, actual: %v", len(oldMigr.Status.Volumes), len(updatedMigr.Status.Volumes))
	}
	if len(oldMigr.Status.Resources) != len(updatedMigr.Status.Resources) {
		logrus.Errorf("volumes before upgrade: %v", oldMigr.Status.Resources)
		logrus.Errorf("volumes after upgrade: %v", updatedMigr.Status.Resources)
		return fmt.Errorf("volume counts after upgrade does not match, expected: %v, actual: %v", len(oldMigr.Status.Resources), len(updatedMigr.Status.Resources))
	}

	return nil
}
