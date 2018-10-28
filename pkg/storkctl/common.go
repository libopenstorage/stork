package storkctl

import (
	"fmt"
	"io"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func toTimeString(t metav1.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC822)
}

func handleError(err error, errOut io.Writer) {
	msg := fmt.Sprintf("Error: %v", err)
	printMsg(msg, errOut)
	os.Exit(1)
}

func handleEmptyList(out io.Writer) {
	msg := fmt.Sprintf("No resources found.")
	printMsg(msg, out)
}

func printMsg(msg string, out io.Writer) {
	if _, printErr := fmt.Fprintln(out, msg); printErr != nil {
		fmt.Println(msg)
	}
}
