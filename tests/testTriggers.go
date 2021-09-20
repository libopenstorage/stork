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

// RunningTriggers map of events and corresponding interval
var RunningTriggers map[string]time.Duration

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

type emailData struct {
	MasterIP     []string
	NodeInfo     []nodeInfo
	EmailRecords emailRecords
	TriggersInfo []triggerInfo
}

type nodeInfo struct {
	MgmtIP    string
	NodeName  string
	PxVersion string
	Status    string
}

type triggerInfo struct {
	Name     string
	Duration time.Duration
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
	// DeployApps installs new apps
	DeployApps = "deployApps"
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
	// CoreChecker checks if any cores got generated
	CoreChecker = "coreChecker"
)

// TriggerCoreChecker checks if any cores got generated
func TriggerCoreChecker(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CoreChecker,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	context(fmt.Sprintf("checking for core files..."), func() {
		Step(fmt.Sprintf("verifying if core files are present on each node"), func() {
			nodes := node.GetWorkerNodes()
			expect(nodes).NotTo(beEmpty())
			for _, n := range nodes {
				if !n.IsStorageDriverInstalled {
					continue
				}
				logrus.Infof("looking for core files on node %s", n.Name)
				file, err := Inst().N.SystemCheck(n, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				UpdateOutcome(event, err)

				if len(file) != 0 {
					UpdateOutcome(event, fmt.Errorf("[%s] found on node [%s]", file, n.Name))
				}
			}
		})
	})
}

// TriggerDeployNewApps deploys applications in separate namespaces
func TriggerDeployNewApps(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: DeployApps,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	errorChan := make(chan error, errorChannelSize)

	Step("Deploy applications", func() {
		contexts := []*scheduler.Context{}
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			newContexts := ScheduleApplications(fmt.Sprintf("longevity-%d", i), &errorChan)
			contexts = append(contexts, newContexts...)
		}

		for _, ctx := range contexts {
			ctx.SkipVolumeValidation = false
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				UpdateOutcome(event, err)
			}
		}
	})
}

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
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
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

						// GetMaxReplicationFactory is hardcoded to 3
						// if it increases repl 3 to an aggregated 2 volume, it will fail
						// because it would require 6 worker nodes, since
						// number of nodes required = aggregation level * replication factor
						currAggr, err := Inst().V.GetAggregationLevel(v)
						UpdateOutcome(event, err)

						if currAggr > 1 {
							MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
						}
						if currRep == MaxRF {
							errExpected = true
						}
						expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
						err = Inst().V.SetReplicationFactor(v, currRep+1, nil, opts)
						if !errExpected {
							UpdateOutcome(event, err)
						} else {
							if !expect(err).To(haveOccurred()) {
								UpdateOutcome(event, fmt.Errorf("expected HA increase to fail since new repl factor is greater than %v but it did not", MaxRF))
							}
						}
					})
				Step(
					fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)

						if newRepl != expReplMap[v] {
							err = fmt.Errorf("volume has invalid repl value. Expected:%d Actual:%d", expReplMap[v], newRepl)
							UpdateOutcome(event, err)
						}
						if newRepl != expReplMap[v] {
							UpdateOutcome(event,
								fmt.Errorf("actual volume replica %d does not match with expected volume replica %d for volume [%s]", newRepl, expReplMap[v], v.Name))
						}
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
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
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

						if currRep == MinRF {
							errExpected = true
						}
						expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))

						err = Inst().V.SetReplicationFactor(v, currRep-1, nil, opts)
						if !errExpected {
							UpdateOutcome(event, err)

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

						if newRepl != expReplMap[v] {
							UpdateOutcome(event, fmt.Errorf("volume has invalid repl value. Expected:%d Actual:%d", expReplMap[v], newRepl))
						}
						if newRepl != expReplMap[v] {
							UpdateOutcome(event,
								fmt.Errorf("actual volume replica %d does not match with expected volume replica %d for volume [%s]", newRepl, expReplMap[v], v.Name))
						}
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
					taskStep := fmt.Sprintf("crash volume driver on node: %s",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					CrashVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
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
					taskStep := fmt.Sprintf("stop volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					StopVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})

			Step(
				fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name),
				func() {
					taskStep := fmt.Sprintf("starting volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					StartVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
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
						taskStep := fmt.Sprintf("reboot node: %s.", n.MgmtIp)
						event.Event.Type += "<br>" + taskStep
						err := Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
						err := Inst().V.WaitDriverDownOnNode(n)
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String()), func() {

						err := Inst().S.IsNodeReady(n)
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
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

	emailData := emailData{}
	logrus.Infof("Generating email report: %s", time.Now().Format(time.RFC1123))

	var masterNodeList []string
	var pxStatus string

	for _, n := range node.GetMasterNodes() {
		masterNodeList = append(masterNodeList, n.Addresses...)
	}
	emailData.MasterIP = masterNodeList

	for _, n := range node.GetWorkerNodes() {
		status, err := Inst().V.GetNodeStatus(n)
		if err != nil {
			pxStatus = "ERROR GETTING STATUS"
		} else {
			pxStatus = status.String()
		}

		emailData.NodeInfo = append(emailData.NodeInfo, nodeInfo{MgmtIP: n.MgmtIp, NodeName: n.Name,
			PxVersion: n.NodeLabels["PX Version"], Status: pxStatus})
	}

	for k, v := range RunningTriggers {
		emailData.TriggersInfo = append(emailData.TriggersInfo, triggerInfo{Name: k, Duration: v})
	}
	for i := 0; i < eventRing.Len(); i++ {
		record := eventRing.Value
		if record != nil {
			emailData.EmailRecords.Records = append(emailData.EmailRecords.Records, *record.(*EventRecord))
			eventRing.Value = nil
		}
		eventRing = eventRing.Next()
	}

	content, err := prepareEmailBody(emailData)
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

func prepareEmailBody(eventRecords emailData) (string, error) {
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

var htmlTemplate = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<script src="http://ajax.googleapis.com/ajax/libs/jquery/2.0.0/jquery.min.js"></script>
<style>
table {
  border-collapse: collapse;
}
th {
   background-color: #0ca1f0;
   text-align: center;
   padding: 3px;
}
td {
  text-align: center;
  padding: 3px;
}

tbody tr:nth-child(even) {
  background-color: #bac5ca;
}
tbody tr:last-child {
  background-color: #79ab78;
}
@media only screen and (max-width: 500px) {
	.wrapper table {
		width: 100% !important;
	}

	.wrapper .column {
		// make the column full width on small screens and allow stacking
		width: 100% !important;
		display: block !important;
	}
}
</style>
</head>
<body>
<h1>Torpedo Longevity Report</h1>
<hr/>
<h3>SetUp Details</h3>
<p><b>Master IP:</b> {{.MasterIP}}</p>
<table id="pxtable" border=1 width: 50% >
<tr>
   <td align="center"><h4>PX Node IP </h4></td>
   <td align="center"><h4>PX Node Name </h4></td>
   <td align="center"><h4>PX Version </h4></td>
   <td align="center"><h4>PX Status </h4></td>
 </tr>
{{range .NodeInfo}}<tr>
{{range rangeStruct .}} <td>{{.}}</td>
{{end}}</tr>
{{end}}
</table>
<hr/>
<h3>Running Event Details</h3>
<table border=1 width: 50%>
<tr>
   <td align="center"><h4>Trigget Name </h4></td>
   <td align="center"><h4>Interval </h4></td>
 </tr>
{{range .TriggersInfo}}<tr>
{{range rangeStruct .}} <td>{{.}}</td>
{{end}}</tr>
{{end}}
</table>
<hr/>
<h3>Event Details</h3>
<table border=1 width: 100%>
<tr>
   <td class="wrapper" width="600" align="center"><h4>Event </h4></td>
   <td align="center"><h4>Start Time </h4></td>
   <td align="center"><h4>End Time </h4></td>
   <td align="center"><h4>Errors </h4></td>
 </tr>
{{range .EmailRecords.Records}}<tr>
{{range rangeStruct .}} <td>{{.}}</td>
{{end}}</tr>
{{end}}
</table>
<script>
$('#pxtable tr td').each(function(){
  var cellValue = $(this).html();
  
    if (cellValue != "STATUS_OK") {
      $(this).css('background-color','red');
    }
});
</script>
<hr/>
</table>
</body>
</html>`
