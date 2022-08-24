package osutils

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
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
	logrus.Debugf("command output for '%s': %s", cmd.String(), string(output))
	if err != nil {
		return fmt.Errorf("error on executing kubectl command, Err: %+v", err)
	}

	return nil
}

// ExecShell Function to execute local command
func ExecShell(command string) (string, string, error) {
	return ExecShellWithEnv(command)
}

// ExecShellWithEnv Function to execute local command with environment variable as param.
func ExecShellWithEnv(command string, envVars ...string) (string, string, error) {
	var stout, sterr []byte
	cmd := exec.Command("bash", "-c", command)
	logrus.Debugf("Command %s ", command)
	cmd.Env = append(cmd.Env, envVars...)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		logrus.Debugf("Command %s failed to start. Cause: %v", command, err)
		return "", "", err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		stout, _ = copyAndCapture(os.Stdout, stdout)
		wg.Done()
	}()

	sterr, _ = copyAndCapture(os.Stderr, stderr)

	wg.Wait()

	err := cmd.Wait()
	return string(stout), string(sterr), err
}

func copyAndCapture(w io.Writer, r io.Reader) ([]byte, error) {
	var out []byte
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			d := buf[:n]
			out = append(out, d...)
			_, err := w.Write(d)
			if err != nil {
				return out, err
			}
		}
		if err != nil {
			// Read returns io.EOF at the end of file, which is not an error for us
			if err == io.EOF {
				err = nil
			}
			return out, err
		}
	}
}
