package storkctl

import (
	"fmt"
	"io"
	"time"
)

func toTimeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC822)
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
