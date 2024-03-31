package tektoncd

import (
	"context"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	k8smetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// taskOps is an interface to perform task related operations
type taskOps interface {
	// CreateTask creates a new task
	CreateTask(task *tektonv1.Task, namespace string) (*tektonv1.Task, error)
	// ListTasks lists all tasks in a namespace
	ListTasks(namespace string) (*tektonv1.TaskList, error)
	// GetTask gets a task by name
	GetTask(namespace, name string) (*tektonv1.Task, error)
	// DeleteTask deletes a task by name
	DeleteTask(namespace, name string) error
	// UpdateTask updates a task
	UpdateTask(*tektonv1.Task) (*tektonv1.Task, error)
}

// taskRunOps is an interface to perform task run related operations
type taskRunOps interface {
	// CreateTaskRun creates a new task run
	CreateTaskRun(*tektonv1.TaskRun) (*tektonv1.TaskRun, error)
	// ListTaskRuns lists all task runs in a namespace
	ListTaskRuns(namespace string) (*tektonv1.TaskRunList, error)
	// GetTaskRun gets a task run by name
	GetTaskRun(namespace, name string) (*tektonv1.TaskRun, error)
	// DeleteTaskRun deletes a task run by name
	DeleteTaskRun(namespace, name string) error
	// UpdateTaskRun updates a task run
	UpdateTaskRun(*tektonv1.TaskRun) (*tektonv1.TaskRun, error)
}

// CreateTask creates a new task on a given namespace
func (c *Client) CreateTask(task *tektonv1.Task, namespace string) (*tektonv1.Task, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1TaskClient.Create(context.TODO(), task, k8smetav1.CreateOptions{})
}

// ListTasks lists all tasks in a namespace
func (c *Client) ListTasks(namespace string) (*tektonv1.TaskList, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1TaskClient.List(context.TODO(), k8smetav1.ListOptions{})
}

// GetTask gets a task by name on a given namespace
func (c *Client) GetTask(namespace, name string) (*tektonv1.Task, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1TaskClient.Get(context.TODO(), name, k8smetav1.GetOptions{})
}

// DeleteTask deletes a task by name on a given namespace
func (c *Client) DeleteTask(namespace, name string) error {
	if err := c.initClient(namespace); err != nil {
		return err
	}
	return c.V1TaskClient.Delete(context.TODO(), name, k8smetav1.DeleteOptions{})
}

// UpdateTask updates a task
func (c *Client) UpdateTask(task *tektonv1.Task) (*tektonv1.Task, error) {
	if err := c.initClient(task.Namespace); err != nil {
		return nil, err
	}
	return c.V1TaskClient.Update(context.TODO(), task, k8smetav1.UpdateOptions{})
}

// CreateTaskRun creates a new task run
func (c *Client) CreateTaskRun(taskRun *tektonv1.TaskRun) (*tektonv1.TaskRun, error) {
	if err := c.initClient(taskRun.Namespace); err != nil {
		return nil, err
	}
	return c.V1TaskRunClient.Create(context.TODO(), taskRun, k8smetav1.CreateOptions{})
}

// ListTaskRuns lists all task runs in a namespace
func (c *Client) ListTaskRuns(namespace string) (*tektonv1.TaskRunList, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1TaskRunClient.List(context.TODO(), k8smetav1.ListOptions{})
}

// GetTaskRun gets a task run by name on a given namespace
func (c *Client) GetTaskRun(namespace, name string) (*tektonv1.TaskRun, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1TaskRunClient.Get(context.TODO(), name, k8smetav1.GetOptions{})
}

// DeleteTaskRun deletes a task run by name on a given namespace
func (c *Client) DeleteTaskRun(namespace, name string) error {
	if err := c.initClient(namespace); err != nil {
		return err
	}
	return c.V1TaskRunClient.Delete(context.TODO(), name, k8smetav1.DeleteOptions{})
}

// UpdateTaskRun updates a task run
func (c *Client) UpdateTaskRun(taskRun *tektonv1.TaskRun) (*tektonv1.TaskRun, error) {
	if err := c.initClient(taskRun.Namespace); err != nil {
		return nil, err
	}
	return c.V1TaskRunClient.Update(context.TODO(), taskRun, k8smetav1.UpdateOptions{})
}
