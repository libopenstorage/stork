package storkctl

import (
	"fmt"
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

func handleError(err error) {
	fmt.Printf("Error: %v\n", err)
	os.Exit(1)
}

func handleEmptyList() {
	fmt.Println("No resources found.")
	os.Exit(0)
}
