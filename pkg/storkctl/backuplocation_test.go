// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNoBackupLocation(t *testing.T) {
	cmdArgs := []string{"get", "backuplocation"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestBackupLocationNotFound(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"get", "backuplocation", "testlocation"}
	expected := `Error from server (NotFound): backuplocations.stork.libopenstorage.org "testlocation" not found`
	testCommon(t, cmdArgs, nil, expected, true)

	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "testlocation1",
			Namespace: "default",
		},
		Location: storkv1.BackupLocationItem{
			Type: storkv1.BackupLocationS3,
		},
	}
	_, err := k8s.Instance().CreateBackupLocation(backupLocation)
	require.NoError(t, err, "Error creating backuplocation")

	expected = `Error from server (NotFound): backuplocations.stork.libopenstorage.org "testlocation" not found`
	testCommon(t, cmdArgs, nil, expected, true)

	expected = "\nS3:\n---\n" +
		"NAME            PATH      ACCESS-KEY-ID   SECRET-ACCESS-KEY   REGION      ENDPOINT           SSL-DISABLED\n" +
		"testlocation1                             <HIDDEN>            us-east-1   s3.amazonaws.com   false\n"
	cmdArgs = []string{"get", "backuplocation", "testlocation1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestS3BackupLocation(t *testing.T) {
	defer resetTest()

	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "s3location",
			Namespace: "default",
		},
		Location: storkv1.BackupLocationItem{
			Type: storkv1.BackupLocationS3,
		},
	}
	_, err := k8s.Instance().CreateBackupLocation(backupLocation)
	require.NoError(t, err, "Error creating backuplocation")

	expected := "\nS3:\n---\n" +
		"NAME         PATH      ACCESS-KEY-ID   SECRET-ACCESS-KEY   REGION      ENDPOINT           SSL-DISABLED\n" +
		"s3location                             <HIDDEN>            us-east-1   s3.amazonaws.com   false\n"
	cmdArgs := []string{"get", "backuplocation", "s3location"}
	testCommon(t, cmdArgs, nil, expected, false)

	backupLocation.Location.Path = "testpath"
	backupLocation.Location.S3Config = &storkv1.S3Config{
		AccessKeyID:     "accesskey",
		SecretAccessKey: "secretKey",
		Endpoint:        "127.0.0.1",
		DisableSSL:      true,
		Region:          "us-west-1",
	}
	_, err = k8s.Instance().UpdateBackupLocation(backupLocation)
	require.NoError(t, err, "Error updating backuplocation")

	expected = "\nS3:\n---\n" +
		"NAME         PATH       ACCESS-KEY-ID   SECRET-ACCESS-KEY   REGION      ENDPOINT    SSL-DISABLED\n" +
		"s3location   testpath   accesskey       <HIDDEN>            us-west-1   127.0.0.1   true\n"
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "\nS3:\n---\n" +
		"NAME         PATH       ACCESS-KEY-ID   SECRET-ACCESS-KEY   REGION      ENDPOINT    SSL-DISABLED\n" +
		"s3location   testpath   accesskey       secretKey           us-west-1   127.0.0.1   true\n"
	cmdArgs = []string{"get", "backuplocation", "s3location", "-s"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestAzureBackupLocation(t *testing.T) {
	defer resetTest()

	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "azurelocation",
			Namespace: "default",
		},
		Location: storkv1.BackupLocationItem{
			Type: storkv1.BackupLocationAzure,
		},
	}
	_, err := k8s.Instance().CreateBackupLocation(backupLocation)
	require.NoError(t, err, "Error creating backuplocation")

	expected := "\nAzureBlob:\n----------\n" +
		"NAME            PATH      STORAGE-ACCOUNT-NAME   STORAGE-ACCOUNT-KEY\n" +
		"azurelocation                                    <HIDDEN>\n"
	cmdArgs := []string{"get", "backuplocation", "azurelocation"}
	testCommon(t, cmdArgs, nil, expected, false)

	backupLocation.Location.Path = "testpath"
	backupLocation.Location.AzureConfig = &storkv1.AzureConfig{
		StorageAccountName: "accountname",
		StorageAccountKey:  "accountkey",
	}
	_, err = k8s.Instance().UpdateBackupLocation(backupLocation)
	require.NoError(t, err, "Error updating backuplocation")

	expected = "\nAzureBlob:\n----------\n" +
		"NAME            PATH       STORAGE-ACCOUNT-NAME   STORAGE-ACCOUNT-KEY\n" +
		"azurelocation   testpath   accountname            <HIDDEN>\n"
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "\nAzureBlob:\n----------\n" +
		"NAME            PATH       STORAGE-ACCOUNT-NAME   STORAGE-ACCOUNT-KEY\n" +
		"azurelocation   testpath   accountname            accountkey\n"
	cmdArgs = []string{"get", "backuplocation", "azurelocation", "-s"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGoogleBackupLocation(t *testing.T) {
	defer resetTest()

	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "googlelocation",
			Namespace: "default",
		},
		Location: storkv1.BackupLocationItem{
			Type: storkv1.BackupLocationGoogle,
		},
	}
	_, err := k8s.Instance().CreateBackupLocation(backupLocation)
	require.NoError(t, err, "Error creating backuplocation")

	expected := "\nGoogleCloudStorage:\n-------------------\n" +
		"NAME             PATH      PROJECT-ID\n" +
		"googlelocation             \n"
	cmdArgs := []string{"get", "backuplocation", "googlelocation"}
	testCommon(t, cmdArgs, nil, expected, false)

	backupLocation.Location.Path = "testpath"
	backupLocation.Location.GoogleConfig = &storkv1.GoogleConfig{
		ProjectID: "testproject",
	}
	_, err = k8s.Instance().UpdateBackupLocation(backupLocation)
	require.NoError(t, err, "Error updating backuplocation")

	expected = "\nGoogleCloudStorage:\n-------------------\n" +
		"NAME             PATH       PROJECT-ID\n" +
		"googlelocation   testpath   testproject\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestAllBackupLocation(t *testing.T) {
	_, err := k8s.Instance().CreateNamespace("s3", nil)
	require.NoError(t, err, "Error creating s3 namespace")

	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "s3location",
			Namespace: "s3",
		},
		Location: storkv1.BackupLocationItem{
			Type: storkv1.BackupLocationS3,
			Path: "s3path",
			S3Config: &storkv1.S3Config{
				AccessKeyID:     "accesskey",
				SecretAccessKey: "secretKey",
				Endpoint:        "127.0.0.1",
				DisableSSL:      true,
				Region:          "us-west-1",
			},
		},
	}
	_, err = k8s.Instance().CreateBackupLocation(backupLocation)
	require.NoError(t, err, "Error creating backuplocation")

	_, err = k8s.Instance().CreateNamespace("azure", nil)
	require.NoError(t, err, "Error creating azure namespace")

	backupLocation = &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "azurelocation",
			Namespace: "azure",
		},
		Location: storkv1.BackupLocationItem{
			Path: "azurepath",
			Type: storkv1.BackupLocationAzure,
			AzureConfig: &storkv1.AzureConfig{
				StorageAccountName: "accountname",
				StorageAccountKey:  "accountkey",
			},
		},
	}
	_, err = k8s.Instance().CreateBackupLocation(backupLocation)
	require.NoError(t, err, "Error creating backuplocation")

	_, err = k8s.Instance().CreateNamespace("google", nil)
	require.NoError(t, err, "Error creating google namespace")

	backupLocation = &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "googlelocation",
			Namespace: "google",
		},
		Location: storkv1.BackupLocationItem{
			Type: storkv1.BackupLocationGoogle,
			Path: "testpath",
			GoogleConfig: &storkv1.GoogleConfig{
				ProjectID: "testproject",
			},
		},
	}
	_, err = k8s.Instance().CreateBackupLocation(backupLocation)
	require.NoError(t, err, "Error creating backuplocation")

	expected := "\nAzureBlob:\n----------\n" +
		"NAME            PATH        STORAGE-ACCOUNT-NAME   STORAGE-ACCOUNT-KEY\n" +
		"azurelocation   azurepath   accountname            <HIDDEN>\n"
	cmdArgs := []string{"get", "backuplocation", "azurelocation", "-n", "azure"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "\nS3:\n---\n" +
		"NAME         PATH      ACCESS-KEY-ID   SECRET-ACCESS-KEY   REGION      ENDPOINT    SSL-DISABLED\n" +
		"s3location   s3path    accesskey       <HIDDEN>            us-west-1   127.0.0.1   true\n"
	cmdArgs = []string{"get", "backuplocation", "s3location", "-n", "s3"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "\nGoogleCloudStorage:\n-------------------\n" +
		"NAME             PATH       PROJECT-ID\n" +
		"googlelocation   testpath   testproject\n"
	cmdArgs = []string{"get", "backuplocation", "googlelocation", "-n", "google"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "\nS3:\n---\n" +
		"NAMESPACE   NAME         PATH      ACCESS-KEY-ID   SECRET-ACCESS-KEY   REGION      ENDPOINT    SSL-DISABLED\n" +
		"s3          s3location   s3path    accesskey       <HIDDEN>            us-west-1   127.0.0.1   true\n\n" +
		"GoogleCloudStorage:\n-------------------\n" +
		"NAMESPACE   NAME             PATH       PROJECT-ID\n" +
		"google      googlelocation   testpath   testproject\n\n" +
		"AzureBlob:\n----------\n" +
		"NAMESPACE   NAME            PATH        STORAGE-ACCOUNT-NAME   STORAGE-ACCOUNT-KEY\n" +
		"azure       azurelocation   azurepath   accountname            <HIDDEN>\n"
	cmdArgs = []string{"get", "backuplocation", "--all-namespaces"}
	testCommon(t, cmdArgs, nil, expected, false)
}
