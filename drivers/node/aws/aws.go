package aws

import (
	"fmt"
	"os"

	aws_pkg "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
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
	instances   []*ec2.Instance
}

func (a *aws) String() string {
	return DriverName
}

func (a *aws) Init(sched string) error {
	var err error
	sess := session.Must(session.NewSession())
	a.session = sess
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
	instances, err := a.getAllInstances()
	if err != nil {
		return err
	}
	a.instances = instances
	return nil
}

func (a *aws) TestConnection(n node.Node, options node.ConnectionOpts) error {
	return nil
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
