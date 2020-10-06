package tests

import (
	"bytes"
	"fmt"
	"math"
	"os/exec"
	"reflect"
	"strings"
	"text/template"
	"time"

	"container/ring"

	"github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"

	"github.com/portworx/torpedo/pkg/email"
	"github.com/sirupsen/logrus"
)

const (
	subject = "Torpedo Longevity Report"
	from    = "wilkins@portworx.com"

	// EmailRecipientsConfigMapField is field in config map whos value is comma
	// seperated list of email IDs which will recieve email notifications about logivity
	EmailRecipientsConfigMapField = "emailRecipients"
	// DefaultEmailRecipient is list of email IDs that will recieve email
	// notifications when no EmailRecipientsConfigMapField field present in configMap
	DefaultEmailRecipient = "test@portworx.com"
	// SendGridEmailAPIKeyField is field in config map which stores the SendGrid Email API key
	SendGridEmailAPIKeyField = "sendGridAPIKey"
)
const (
	validateReplicationUpdateTimeout = 2 * time.Hour
	errorChannelSize                 = 10
)

// EmailRecipients list of email IDs to send email to
var EmailRecipients []string

// SendGridEmailAPIKey holds API key used to interact
// with SendGrid Email APIs
var SendGridEmailAPIKey string

// Event describes type of test trigger
type Event struct {
	ID   string
	Type string
}

// EventRecord recodes which event took
// place at what time with what outcome
type EventRecord struct {
	Event   Event
	Start   string
	End     string
	Outcome []error
}

// eventRing is circular buffer to store
// events for sending email notifications
var eventRing *ring.Ring

// emailRecords stores events for rendering
// email template
type emailRecords struct {
	Records []EventRecord
}

// GenerateUUID generates unique ID
func GenerateUUID() string {
	uuidbyte, _ := exec.Command("uuidgen").Output()
	return strings.TrimSpace(string(uuidbyte))
}

// UpdateOutcome updates outcome based on error
func UpdateOutcome(event *EventRecord, err error) {
	if err != nil {
		event.Outcome = append(event.Outcome, err)
	}
}

const (
	// HAIncrease performs repl-add
	HAIncrease = "haIncrease"
	// HADecrease performs repl-reduce
	HADecrease = "haDecrease"
	// AppTaskDown deletes application task for all contexts
	AppTaskDown = "appTaskDown"
	// RestartVolDriver restart volume driver
	RestartVolDriver = "restartVolDriver"
	// CrashVolDriver crashes volume driver
	CrashVolDriver = "crashVolDriver"
	// RebootNode reboots alll nodes one by one
	RebootNode = "rebootNode"
	// EmailReporter notifies via email outcome of past events
	EmailReporter = "emailReporter"
)

// TriggerHAIncrease peforms repl-add on all volumes of given contexts
func TriggerHAIncrease(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HAIncrease,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	expReplMap := make(map[*volume.Volume]int64)
	Step("get volumes for all apps in test and increase replication factor", func() {
		time.Sleep(10 * time.Minute)
		for _, ctx := range contexts {
			var appVolumes []*volume.Volume
			var err error
			Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				expect(appVolumes).NotTo(beEmpty())
			})
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
			}
			for _, v := range appVolumes {
				MaxRF := Inst().V.GetMaxReplicationFactor()

				Step(
					fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v),
					func() {
						errExpected := false
						currRep, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
						expect(err).NotTo(haveOccurred())
						// GetMaxReplicationFactory is hardcoded to 3
						// if it increases repl 3 to an aggregated 2 volume, it will fail
						// because it would require 6 worker nodes, since
						// number of nodes required = aggregation level * replication factor
						currAggr, err := Inst().V.GetAggregationLevel(v)
						UpdateOutcome(event, err)
						expect(err).NotTo(haveOccurred())
						if currAggr > 1 {
							MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
						}
						if currRep == MaxRF {
							errExpected = true
						}
						expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
						err = Inst().V.SetReplicationFactor(v, currRep+1, opts)
						if !errExpected {
							UpdateOutcome(event, err)
							expect(err).NotTo(haveOccurred())
						} else {
							if !expect(err).To(haveOccurred()) {
								UpdateOutcome(event, fmt.Errorf("Expected HA increase to fail since new repl factor is greater than %v but it did not", MaxRF))
							}
						}
					})
				Step(
					fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
						expect(err).NotTo(haveOccurred())
						if newRepl != expReplMap[v] {
							err = fmt.Errorf("volume has invalid repl value. Expected:%d Actual:%d", expReplMap[v], newRepl)
							UpdateOutcome(event, err)
						}
						expect(newRepl).To(equal(expReplMap[v]))
					})
			}
			Step(fmt.Sprintf("validating context after increasing HA for app: %s",
				ctx.App.Key), func() {
				errorChan := make(chan error, errorChannelSize)
				ctx.SkipVolumeValidation = false
				ValidateContext(ctx, &errorChan)
				for err := range errorChan {
					UpdateOutcome(event, err)
				}
			})
		}
	})
}

// TriggerHADecrease performs repl-reduce on all volumes of given contexts
func TriggerHADecrease(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HADecrease,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	expReplMap := make(map[*volume.Volume]int64)
	Step("get volumes for all apps in test and decrease replication factor", func() {
		for _, ctx := range contexts {
			var appVolumes []*volume.Volume
			var err error
			Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				expect(appVolumes).NotTo(beEmpty())
			})
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
			}
			for _, v := range appVolumes {
				MinRF := Inst().V.GetMinReplicationFactor()

				Step(
					fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v),
					func() {
						errExpected := false
						currRep, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
						expect(err).NotTo(haveOccurred())

						if currRep == MinRF {
							errExpected = true
						}
						expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))

						err = Inst().V.SetReplicationFactor(v, currRep-1, opts)
						if !errExpected {
							UpdateOutcome(event, err)
							expect(err).NotTo(haveOccurred())
						} else {
							if !expect(err).To(haveOccurred()) {
								UpdateOutcome(event, fmt.Errorf("Expected HA reduce to fail since new repl factor is less than %v but it did not", MinRF))
							}
						}

					})
				Step(
					fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
						ctx.App.Key, v),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
						expect(err).NotTo(haveOccurred())
						if newRepl != expReplMap[v] {
							UpdateOutcome(event, fmt.Errorf("volume has invalid repl value. Expected:%d Actual:%d", expReplMap[v], newRepl))
						}
						expect(newRepl).To(equal(expReplMap[v]))
					})
			}
			Step(fmt.Sprintf("validating context after reducing HA for app: %s",
				ctx.App.Key), func() {
				errorChan := make(chan error, errorChannelSize)
				ctx.SkipVolumeValidation = false
				ValidateContext(ctx, &errorChan)
				for err := range errorChan {
					UpdateOutcome(event, err)
				}
			})
		}
	})
}

// TriggerAppTaskDown deletes application task for all contexts
func TriggerAppTaskDown(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTaskDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	for _, ctx := range contexts {
		Step(fmt.Sprintf("delete tasks for app: [%s]", ctx.App.Key), func() {
			err := Inst().S.DeleteTasks(ctx, nil)
			UpdateOutcome(event, err)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("validating context after delete tasks for app: [%s]",
			ctx.App.Key), func() {
			errorChan := make(chan error, errorChannelSize)
			ctx.SkipVolumeValidation = false
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				UpdateOutcome(event, err)
			}
		})
	}
}

// TriggerCrashVolDriver crashes vol driver
func TriggerCrashVolDriver(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CrashVolDriver,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("crash volume driver in all nodes", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("crash volume driver %s on node: %v",
					Inst().V.String(), appNode.Name),
				func() {
					CrashVolDriverAndWait([]node.Node{appNode})
				})
		}
	})
}

// TriggerRestartVolDriver restarts volume driver and validates app
func TriggerRestartVolDriver(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RestartVolDriver,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("get nodes bounce volume driver", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("stop volume driver %s on node: %s",
					Inst().V.String(), appNode.Name),
				func() {
					StopVolDriverAndWait([]node.Node{appNode})
				})

			Step(
				fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name),
				func() {
					StartVolDriverAndWait([]node.Node{appNode})
				})

			Step("Giving few seconds for volume driver to stabilize", func() {
				time.Sleep(20 * time.Second)
			})

			for _, ctx := range contexts {
				Step(fmt.Sprintf("RestartVolDriver: validating app [%s]", ctx.App.Key), func() {
					errorChan := make(chan error, errorChannelSize)
					ValidateContext(ctx, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			}
		}
	})
}

// TriggerRebootNodes reboots node on which apps are running
func TriggerRebootNodes(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RebootNode,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("get all nodes and reboot one by one", func() {
		nodesToReboot := node.GetWorkerNodes()

		// Reboot node and check driver status
		Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
			// TODO: Below is the same code from existing nodeReboot test
			for _, n := range nodesToReboot {
				if n.IsStorageDriverInstalled {
					Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
						err := Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
						err := Inst().V.WaitDriverDownOnNode(n)
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String()), func() {

						err := Inst().S.IsNodeReady(n)
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step("validate apps", func() {
						for _, ctx := range contexts {
							Step(fmt.Sprintf("RebootNode: validating app [%s]", ctx.App.Key), func() {
								errorChan := make(chan error, errorChannelSize)
								ValidateContext(ctx, &errorChan)
								for err := range errorChan {
									UpdateOutcome(event, err)
								}
							})
						}
					})
				}
			}
		})
	})
}

// CollectEventRecords collects eventRecords from channel
// and stores in buffer for future email notifications
func CollectEventRecords(recordChan *chan *EventRecord) {
	eventRing = ring.New(100)
	for eventRecord := range *recordChan {
		eventRing.Value = eventRecord
		eventRing = eventRing.Next()
	}
}

// TriggerEmailReporter sends email with all reported errors
func TriggerEmailReporter(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	// emailRecords stores events to be notified
	emailRecords := emailRecords{}
	logrus.Infof("Generating email report: %s", time.Now().Format(time.RFC1123))
	for i := 0; i < eventRing.Len(); i++ {
		record := eventRing.Value
		if record != nil {
			emailRecords.Records = append(emailRecords.Records, *record.(*EventRecord))
			eventRing.Value = nil
		}
		eventRing = eventRing.Next()
	}

	content, err := prepareEmailBody(emailRecords)
	if err != nil {
		logrus.Errorf("Failed to prepare email body. Error: [%v]", err)
	}

	emailDetails := &email.Email{
		Subject:        subject,
		Content:        content,
		From:           from,
		To:             EmailRecipients,
		SendGridAPIKey: SendGridEmailAPIKey,
	}

	err = emailDetails.SendEmail()
	if err != nil {
		logrus.Errorf("Failed to send out email, because of Error: %q", err)
	}
}

func prepareEmailBody(eventRecords emailRecords) (string, error) {
	var err error
	t := template.New("t").Funcs(templateFuncs)
	t, err = t.Parse(htmlTemplate)
	if err != nil {
		logrus.Errorf("Cannot parse HTML template Err: %v", err)
		return "", err
	}
	var buf []byte
	buffer := bytes.NewBuffer(buf)
	err = t.Execute(buffer, eventRecords)
	if err != nil {
		logrus.Errorf("Cannot generate body from values, Err: %v", err)
		return "", err
	}

	return buffer.String(), nil
}

var templateFuncs = template.FuncMap{"rangeStruct": rangeStructer}

func rangeStructer(args ...interface{}) []interface{} {
	if len(args) == 0 {
		return nil
	}

	v := reflect.ValueOf(args[0])
	if v.Kind() != reflect.Struct {
		return nil
	}

	out := make([]interface{}, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		out[i] = v.Field(i).Interface()
	}

	return out
}

var htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<style>
table {
  border-collapse: collapse;
  width: 100%;
}
th {
   background-color: #0ca1f0;
   text-align: center;
   padding: 3px;
}
td {
  text-align: left;
  padding: 3px;
}
tbody tr:nth-child(even) {
	background-color: #bac5ca;
}
tbody tr:last-child {
  background-color: #79ab78;
}
</style>
</head>
<body>
<h1>Torpedo Longevity Report</h1>
<hr/>
<h3>Event Details</h3>
<table border=1>
<tr>
   <td align="center"><h4>Event </h4></td>
   <td align="center"><h4>Start Time </h4></td>
   <td align="center"><h4>End Time </h4></td>
   <td align="center"><h4>Errors </h4></td>
 </tr>
{{range .Records}}<tr>
{{range rangeStruct .}}	<td>{{.}}</td>
{{end}}</tr>
{{end}}
</table>
<hr/>
</table>
</body>
</html>
`
