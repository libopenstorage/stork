package tektoncd

import (
	"context"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	k8smetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// pipelineOps is an interface to perform pipeline related operations
type pipelineOps interface {
	// CreatePipeline creates a new pipeline
	CreatePipeline(pipeline *tektonv1.Pipeline, namespace string) (*tektonv1.Pipeline, error)
	// ListPipelines lists all pipelines in a namespace
	ListPipelines(namespace string) (*tektonv1.PipelineList, error)
	// GetPipeline gets a pipeline by name
	GetPipeline(namespace, name string) (*tektonv1.Pipeline, error)
	// DeletePipeline deletes a pipeline by name
	DeletePipeline(namespace, name string) error
	// UpdatePipeline updates a pipeline
	UpdatePipeline(*tektonv1.Pipeline) (*tektonv1.Pipeline, error)
}

// pipelineRunOps is an interface to perform pipeline run related operations
type pipelineRunOps interface {
	// CreatePipelineRun creates a new pipeline run
	CreatePipelineRun(*tektonv1.PipelineRun) (*tektonv1.PipelineRun, error)
	// ListPipelineRuns lists all pipeline runs in a namespace
	ListPipelineRuns(namespace string) (*tektonv1.PipelineRunList, error)
	// GetPipelineRun gets a pipeline run by name
	GetPipelineRun(namespace, name string) (*tektonv1.PipelineRun, error)
	// DeletePipelineRun deletes a pipeline run by name
	DeletePipelineRun(namespace, name string) error
	// UpdatePipelineRun updates a pipeline run
	UpdatePipelineRun(*tektonv1.PipelineRun) (*tektonv1.PipelineRun, error)
}

// CreatePipeline creates a new pipeline on a given namespace
func (c Client) CreatePipeline(pipeline *tektonv1.Pipeline, namespace string) (*tektonv1.Pipeline, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineClient.Create(context.TODO(), pipeline, k8smetav1.CreateOptions{})
}

// ListPipelines lists all pipelines in a namespace
func (c Client) ListPipelines(namespace string) (*tektonv1.PipelineList, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineClient.List(context.TODO(), k8smetav1.ListOptions{})
}

// GetPipeline gets a pipeline by name on a given namespace
func (c Client) GetPipeline(namespace, name string) (*tektonv1.Pipeline, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineClient.Get(context.TODO(), name, k8smetav1.GetOptions{})
}

// DeletePipeline deletes a pipeline by name on a given namespace
func (c Client) DeletePipeline(namespace, name string) error {
	if err := c.initClient(namespace); err != nil {
		return err
	}
	return c.V1PipelineClient.Delete(context.TODO(), name, k8smetav1.DeleteOptions{})
}

// UpdatePipeline updates a pipeline
func (c Client) UpdatePipeline(pipeline *tektonv1.Pipeline) (*tektonv1.Pipeline, error) {
	if err := c.initClient(pipeline.Namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineClient.Update(context.TODO(), pipeline, k8smetav1.UpdateOptions{})
}

// CreatePipelineRun creates a new pipeline run
func (c Client) CreatePipelineRun(pipelineRun *tektonv1.PipelineRun) (*tektonv1.PipelineRun, error) {
	if err := c.initClient(pipelineRun.Namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineRunClient.Create(context.TODO(), pipelineRun, k8smetav1.CreateOptions{})
}

// ListPipelineRuns lists all pipeline runs in a namespace
func (c Client) ListPipelineRuns(namespace string) (*tektonv1.PipelineRunList, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineRunClient.List(context.TODO(), k8smetav1.ListOptions{})
}

// GetPipelineRun gets a pipeline run by name on a given namespace
func (c Client) GetPipelineRun(namespace, name string) (*tektonv1.PipelineRun, error) {
	if err := c.initClient(namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineRunClient.Get(context.TODO(), name, k8smetav1.GetOptions{})
}

// DeletePipelineRun deletes a pipeline run by name on a given namespace
func (c Client) DeletePipelineRun(namespace, name string) error {
	if err := c.initClient(namespace); err != nil {
		return err
	}
	return c.V1PipelineRunClient.Delete(context.TODO(), name, k8smetav1.DeleteOptions{})
}

// UpdatePipelineRun updates a pipeline run
func (c Client) UpdatePipelineRun(pipelineRun *tektonv1.PipelineRun) (*tektonv1.PipelineRun, error) {
	if err := c.initClient(pipelineRun.Namespace); err != nil {
		return nil, err
	}
	return c.V1PipelineRunClient.Update(context.TODO(), pipelineRun, k8smetav1.UpdateOptions{})
}
