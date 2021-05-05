package aws

import (
	"fmt"
	"os"
	"strings"
	"time"

	aws_pkg "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
)

const (
	// DriverName is the name of the aws driver
	DriverName = "aws"
)

type aws struct {
	node.Driver
	session     *session.Session
	credentials *credentials.Credentials
	config      *aws_pkg.Config
	region      string
	svc         *ec2.EC2
	svcSsm      *ssm.SSM
	instances   []*ec2.Instance
}

func (a *aws) String() string {
	return DriverName
}

func (a *aws) Init(nodeOpts node.InitOptions) error {
	var err error
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	creds := credentials.NewEnvCredentials()
	a.credentials = creds
	a.region = os.Getenv("AWS_REGION")
	if a.region == "" {
		return fmt.Errorf("Env AWS_REGION not found")
	}
	config := &aws_pkg.Config{Region: aws_pkg.String(a.region)}
	config.WithCredentials(creds)
	a.config = config
	svc := ec2.New(sess, config)
	a.svc = svc
	a.svcSsm = ssm.New(sess, aws_pkg.NewConfig().WithRegion(a.region))
	a.session = sess
	instances, err := a.getAllInstances()
	if err != nil {
		return err
	}
	a.instances = instances
	nodes := node.GetWorkerNodes()
	for _, n := range nodes {
		if err := a.TestConnection(n, node.ConnectionOpts{
			Timeout:         1 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		}); err != nil {
			return &node.ErrFailedToTestConnection{
				Node:  n,
				Cause: err.Error(),
			}
		}
	}
	return nil
}

func (a *aws) TestConnection(n node.Node, options node.ConnectionOpts) error {
	var err error
	instanceID, err := a.getNodeIDByPrivAddr(n)
	if err != nil {
		return &node.ErrFailedToTestConnection{
			Node:  n,
			Cause: fmt.Sprintf("failed to get instance ID for connection due to: %v", err),
		}
	}
	command := "uptime"
	param := make(map[string][]*string)
	param["commands"] = []*string{
		aws_pkg.String(command),
	}
	sendCommandInput := &ssm.SendCommandInput{
		Comment:      aws_pkg.String(command),
		DocumentName: aws_pkg.String("AWS-RunShellScript"),
		Parameters:   param,
		InstanceIds: []*string{
			aws_pkg.String(instanceID),
		},
	}
	sendCommandOutput, err := a.svcSsm.SendCommand(sendCommandInput)
	if err != nil {
		return &node.ErrFailedToTestConnection{
			Node:  n,
			Cause: fmt.Sprintf("failed to send command to instance %s: %v", instanceID, err),
		}
	}
	if sendCommandOutput.Command == nil || sendCommandOutput.Command.CommandId == nil {
		return fmt.Errorf("No command returned after sending command to %s", instanceID)
	}
	listCmdsInput := &ssm.ListCommandInvocationsInput{
		CommandId: sendCommandOutput.Command.CommandId,
	}
	t := func() (interface{}, bool, error) {
		return "", true, a.connect(n, listCmdsInput)
	}

	if _, err := task.DoRetryWithTimeout(t, options.Timeout, options.TimeBeforeRetry); err != nil {
		return &node.ErrFailedToTestConnection{
			Node:  n,
			Cause: err.Error(),
		}
	}
	return err
}

func (a *aws) connect(n node.Node, listCmdsInput *ssm.ListCommandInvocationsInput) error {
	var status string
	listCmdInvsOutput, _ := a.svcSsm.ListCommandInvocations(listCmdsInput)
	for _, cmd := range listCmdInvsOutput.CommandInvocations {
		status = strings.TrimSpace(*cmd.StatusDetails)
		if status == "Success" {
			return nil
		}
	}
	return &node.ErrFailedToTestConnection{
		Node:  n,
		Cause: fmt.Sprintf("Failed to connect. Command status is %s", status),
	}
}

func (a *aws) RebootNode(n node.Node, options node.RebootNodeOpts) error {
	var err error
	instanceID, err := a.getNodeIDByPrivAddr(n)
	if err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get instance ID due to: %v", err),
		}
	}
	//Reboot the instance by its InstanceID
	rebootInput := &ec2.RebootInstancesInput{
		InstanceIds: []*string{
			aws_pkg.String(instanceID),
		},
	}
	_, err = a.svc.RebootInstances(rebootInput)
	if err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to reboot instance due to: %v", err),
		}
	}

	return nil
}

func (a *aws) ShutdownNode(n node.Node, options node.ShutdownNodeOpts) error {
	return nil
}

// TODO add AWS implementation for this
func (a *aws) FindFiles(path string, n node.Node, options node.FindOpts) (string, error) {
	return "", nil
}

// TODO implement for AWS
func (a *aws) Systemctl(n node.Node, service string, options node.SystemctlOpts) error {
	return nil
}

func (a *aws) getAllInstances() ([]*ec2.Instance, error) {
	instances := []*ec2.Instance{}
	params := &ec2.DescribeInstancesInput{}
	resp, err := a.svc.DescribeInstances(params)
	if err != nil {
		return instances, fmt.Errorf("there was an error listing instances in %s. Error: %q", a.region, err.Error())
	}
	reservations := resp.Reservations
	for _, resv := range reservations {
		for _, ins := range resv.Instances {
			instances = append(instances, ins)
		}
	}
	return instances, err
}

func (a *aws) getNodeIDByPrivAddr(n node.Node) (string, error) {
	for _, i := range a.instances {
		for _, addr := range n.Addresses {
			if aws_pkg.StringValue(i.PrivateIpAddress) == addr {
				return aws_pkg.StringValue(i.InstanceId), nil
			}
		}
	}
	return "", fmt.Errorf("Failed to get instanceID of %s by privateIP", n.Name)
}

func init() {
	a := &aws{
		Driver: node.NotSupportedDriver,
	}
	node.Register(DriverName, a)
}
