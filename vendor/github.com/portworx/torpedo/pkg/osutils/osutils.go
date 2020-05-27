package osutils

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"strings"
)

// Wget runs wget command
func Wget(URL string, filename string, verifyFile bool) error {
	//fullEndpointURL := fmt.Sprintf("%s/%s/upgrade", endpointURL, endpointVersion)
	if URL == "" {
		return fmt.Errorf("no URL supplied for wget command")
	}
	cmdArgs := []string{}
	if filename != "" {
		cmdArgs = append(cmdArgs, "-O", filename)
	}
	cmdArgs = append(cmdArgs, URL)
	cmd := exec.Command("wget", cmdArgs...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error on executing wget, Err: %v", err)
	}
	if verifyFile {
		file, err := FileExists(filename)
		if err != nil {
			return err
		}
		logrus.Debugf("file %s exists", filename)

		if FileEmpty(file) {
			return fmt.Errorf("file %s is empty", filename)
		}
		logrus.Debugf("file %s is not empty", filename)
	}
	return nil
}

// FileExists returns true if file exists
func FileExists(filename string) (os.FileInfo, error) {
	if filename == "" {
		return nil, fmt.Errorf("no filename supplied for file existence check")
	}
	if file, err := os.Stat(filename); err == nil {
		return file, nil
	} else if os.IsNotExist(err) {
		return nil, nil
	} else {
		return nil, err
	}
}

// FileEmpty verifies if file is empty
func FileEmpty(filename os.FileInfo) bool {
	fileSize := filename.Size()
	if fileSize == 0 {
		return true
	}
	return false
}

// Sh run sh command with arguments
func Sh(arguments []string) error {
	if len(arguments) == 0 {
		return fmt.Errorf("no arguments supplied for sh command")
	}
	cmd := exec.Command("sh", arguments...)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("error on executing sh command, Err: %+v", err)
	}
	// Print and replace all '\n' with new lines
	logrus.Debugf("%s", strings.Replace(string(output[:]), `\n`, "\n", -1))
	return nil
}

// Chmod runs chmod on file with arguments
func Chmod(mode string, filename string) error {
	if filename == "" {
		return fmt.Errorf("no filename supplied for change file mode")
	}
	if mode == "" {
		return fmt.Errorf("no mode supplied for change file mode")
	}
	cmdArgs := []string{mode, filename}
	cmd := exec.Command("chmod", cmdArgs...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error on executing chmod, Err: %v", err)
	}
	logrus.Infof("mode %s changed on file %s", mode, filename)

	return nil
}

// Cat runs cat command on file
func Cat(filename string) ([]byte, error) {
	if filename == "" {
		return nil, fmt.Errorf("no filename supplied for cat command")
	}
	cmd := exec.Command("cat", filename)
	output, err := cmd.Output()
	logrus.Debugf("%s", string(output))
	if err != nil {
		return nil, fmt.Errorf("error on getting context of file %s: %v", filename, err)
	}

	return output, nil
}

// Kubectl run kubectl comamnd with arguments
func Kubectl(arguments []string) error {
	if len(arguments) == 0 {
		return fmt.Errorf("no arguments supplied for kubectl command")
	}
	cmd := exec.Command("kubectl", arguments...)
	output, err := cmd.Output()
	logrus.Debugf("%s", string(output))
	if err != nil {
		return fmt.Errorf("error on executing kubectl command, Err: %+v", err)
	}

	return nil
}
