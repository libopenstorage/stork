package k8s

import (
	"bytes"
	"context"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/sirupsen/logrus"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/cli/values"
	"helm.sh/helm/v3/pkg/downloader"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/repo"
	"helm.sh/helm/v3/pkg/strvals"
)

var settings *cli.EnvSettings

// constants for helm ConfigMap fields
const (
	HelmInstallURL  = "install-url"
	HelmUpgradeURL  = "upgrade-url"
	HelmValues      = "values"
	HelmExtraValues = "extra-values"

	// would overwrite existing fields if provided
	// to handle chart and repo name changes during upgrade
	HelmChartName = "chart-name"
	HelmRepoName  = "repo-name"
)

// HelmSchedule will install the application with helm
func (k *K8s) HelmSchedule(app *spec.AppSpec, appNamespace string, options scheduler.ScheduleOptions) ([]interface{}, error) {
	var specObjects []interface{}
	var err error

	for _, appspec := range app.SpecList {
		if repoInfo, ok := appspec.(*scheduler.HelmRepo); ok {
			err = k.SetHelmParams(app.Key, repoInfo, options)
			if err != nil {
				return nil, err
			}

			var yamlBuf bytes.Buffer
			settings = cli.New()

			// Add helm repo
			err = k.RepoAdd(repoInfo)
			if err != nil {
				return nil, err
			}

			// Update charts from the helm repo
			err = k.RepoUpdate()
			if err != nil {
				return nil, err
			}

			// Install charts
			_, err = k.createNamespace(app, appNamespace, options)
			if err != nil {
				return nil, err
			}

			// Install the chart through helm
			repoInfo.Namespace = appNamespace
			var manifest string
			if !options.Upgrade {
				manifest, err = k.InstallChart(repoInfo)
				if err != nil {
					return nil, err
				}
			} else {
				manifest, err = k.UpgradeChart(repoInfo)
				if err != nil {
					return nil, err
				}
			}

			// Parse the manifest which is a yaml to get the k8s spec objects
			yamlBuf.WriteString(manifest)
			specs, err := k.ParseSpecsFromYamlBuf(&yamlBuf)
			if err != nil {
				return nil, err
			}

			specObjects = append(specObjects, specs...)
		}
	}
	return specObjects, nil
}

// SetHelmParams will set RepoInfo fields of the app from k8s ConfigMap
func (k *K8s) SetHelmParams(appKey string, helmRepo *scheduler.HelmRepo, options scheduler.ScheduleOptions) error {
	configMap, err := k8sCore.GetConfigMap(appKey, "default")
	if err != nil {
		return fmt.Errorf("failed to get config map: %v", err)
	}
	urlKey := HelmInstallURL
	if options.Upgrade {
		// use a different helm URL for staging repo
		urlKey = HelmUpgradeURL
		// add a different helm staging repo
		if repoName, ok := configMap.Data[HelmRepoName]; ok {
			logrus.Debugf("upgrading helm repo name from %s to %s", helmRepo.RepoName, repoName)
			helmRepo.RepoName = repoName
		}
		// chart name could be changed during upgrade, e.g. px-backup -> px-central
		if chartName, ok := configMap.Data[HelmChartName]; ok {
			logrus.Debugf("upgrading helm chart name from %s to %s", helmRepo.ChartName, chartName)
			helmRepo.ChartName = chartName
		}
	}

	if url, ok := configMap.Data[urlKey]; ok {
		helmRepo.URL = url
		logrus.Debugf("helm repo URL set: %s", helmRepo.URL)
	} else {
		return fmt.Errorf("helm repo url '%s' not provided in the configmap %s", urlKey, appKey)
	}

	if values, ok := configMap.Data[HelmValues]; ok {
		helmRepo.Values = values
		logrus.Debugf("helm values set: %s", helmRepo.Values)
	} else {
		return fmt.Errorf("helm install custom values not provided in the configmap %s", appKey)
	}

	// some values are generated during the test, e.g. UI endpoint and OIDC secret
	if extraValues, ok := configMap.Data[HelmExtraValues]; ok && extraValues != "" {
		helmRepo.Values = fmt.Sprintf("%s,%s", helmRepo.Values, extraValues)
		logrus.Debugf("helm extra values added: %s", helmRepo.Values)
	}

	return nil
}

// ParseCharts parses the application spec file having helm repo info
func (k *K8s) ParseCharts(fileName string) (interface{}, error) {
	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	repoInfo := scheduler.HelmRepo{}
	err = yaml.Unmarshal(file, &repoInfo)
	if err != nil {
		return nil, err
	}

	return &repoInfo, nil
}

// RepoAdd adds repo with given name and url
func (k *K8s) RepoAdd(repoInfo *scheduler.HelmRepo) error {
	name := repoInfo.RepoName
	url := repoInfo.URL
	repoFile := settings.RepositoryConfig

	//Ensure the file directory exists as it is required for file locking
	err := os.MkdirAll(filepath.Dir(repoFile), os.ModePerm)
	if err != nil && !os.IsExist(err) {
		return err
	}

	// Acquire a file lock for process synchronization
	fileLock := flock.New(strings.Replace(repoFile, filepath.Ext(repoFile), ".lock", 1))
	lockCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	locked, err := fileLock.TryLockContext(lockCtx, time.Second)
	if err == nil && locked {
		defer fileLock.Unlock()
	}
	if err != nil {
		return err
	}

	b, err := ioutil.ReadFile(repoFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	var f repo.File
	if err := yaml.Unmarshal(b, &f); err != nil {
		return err
	}

	if f.Has(name) {
		logrus.Warnf("repository name (%s) already exists\n", name)
		return nil
	}

	c := repo.Entry{
		Name: name,
		URL:  url,
	}

	r, err := repo.NewChartRepository(&c, getter.All(settings))
	if err != nil {
		return err
	}

	if _, err := r.DownloadIndexFile(); err != nil {
		err := errors.Wrapf(err, "looks like %q is not a valid chart repository or cannot be reached", url)
		return err
	}

	f.Update(&c)

	if err := f.WriteFile(repoFile, 0644); err != nil {
		return err
	}
	logrus.Infof("%q has been added to the repositories", name)
	return nil
}

// RepoUpdate updates charts for all helm repos
func (k *K8s) RepoUpdate() error {
	repoFile := settings.RepositoryConfig

	f, err := repo.LoadFile(repoFile)
	if os.IsNotExist(errors.Cause(err)) || len(f.Repositories) == 0 {
		return fmt.Errorf("No repositories found, need to add one before updating, err: %v", err)
	}
	var repos []*repo.ChartRepository
	for _, cfg := range f.Repositories {
		r, err := repo.NewChartRepository(cfg, getter.All(settings))
		if err != nil {
			return err
		}
		repos = append(repos, r)
	}

	logrus.Debugf("Getting the latest from the chart repositories")
	var wg sync.WaitGroup
	for _, re := range repos {
		wg.Add(1)
		go func(re *repo.ChartRepository) {
			defer wg.Done()
			if _, err := re.DownloadIndexFile(); err != nil {
				logrus.Warnf("Unable to get an update from the %q chart repository (%s):\t%s", re.Config.Name, re.Config.URL, err)
			} else {
				logrus.Debugf("Successfully got an update from the %q chart repository", re.Config.Name)
			}
		}(re)
	}
	wg.Wait()
	logrus.Debugf("RepoUpdate Completed successfully.")
	return nil
}

// InstallChart will install the helm chart
func (k *K8s) InstallChart(repoInfo *scheduler.HelmRepo) (string, error) {
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), repoInfo.Namespace, os.Getenv("HELM_DRIVER"), debug); err != nil {
		return "", err
	}
	client := action.NewInstall(actionConfig)

	if client.Version == "" && client.Devel {
		client.Version = ">0.0.0-0"
	}
	client.ReleaseName = repoInfo.ReleaseName
	cp, err := client.ChartPathOptions.LocateChart(fmt.Sprintf("%s/%s", repoInfo.RepoName, repoInfo.ChartName), settings)
	if err != nil {
		return "", err
	}

	logrus.Debugf("chart install path: %s", cp)

	p := getter.All(settings)
	valueOpts := &values.Options{}
	vals, err := valueOpts.MergeValues(p)
	if err != nil {
		return "", err
	}

	// Add args
	if err := strvals.ParseInto(repoInfo.Values, vals); err != nil {
		return "", errors.Wrap(err, "failed parsing --set data")
	}

	// Check chart dependencies to make sure all are present in /charts
	chartRequested, err := loader.Load(cp)
	if err != nil {
		return "", err
	}

	validInstallableChart, err := isChartInstallable(chartRequested)
	if !validInstallableChart {
		return "", err
	}

	if req := chartRequested.Metadata.Dependencies; req != nil {
		// If CheckDependencies returns an error, we have unfulfilled dependencies.
		// As of Helm 2.4.0, this is treated as a stopping condition:
		// https://github.com/helm/helm/issues/2209
		if err := action.CheckDependencies(chartRequested, req); err != nil {
			if client.DependencyUpdate {
				man := &downloader.Manager{
					Out:              os.Stdout,
					ChartPath:        cp,
					Keyring:          client.ChartPathOptions.Keyring,
					SkipUpdate:       false,
					Getters:          p,
					RepositoryConfig: settings.RepositoryConfig,
					RepositoryCache:  settings.RepositoryCache,
				}
				if err := man.Update(); err != nil {
					return "", err
				}
			} else {
				return "", err
			}
		}
	}

	client.Namespace = repoInfo.Namespace
	release, err := client.Run(chartRequested, vals)
	if err != nil {
		return "", err
	}
	return release.Manifest, nil
}

func isChartInstallable(ch *chart.Chart) (bool, error) {
	switch ch.Metadata.Type {
	case "", "application":
		return true, nil
	}
	return false, errors.Errorf("%s charts are not installable", ch.Metadata.Type)
}

// UpgradeChart will upgrade the release
func (k *K8s) UpgradeChart(repoInfo *scheduler.HelmRepo) (string, error) {
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), repoInfo.Namespace, os.Getenv("HELM_DRIVER"), debug); err != nil {
		return "", err
	}

	client := action.NewUpgrade(actionConfig)
	if client.Version == "" && client.Devel {
		client.Version = ">0.0.0-0"
	}
	cp, err := client.ChartPathOptions.LocateChart(fmt.Sprintf("%s/%s", repoInfo.RepoName, repoInfo.ChartName), settings)
	if err != nil {
		return "", err
	}
	logrus.Debugf("chart upgrade path: %s", cp)

	p := getter.All(settings)
	valueOpts := &values.Options{}
	vals, err := valueOpts.MergeValues(p)
	if err != nil {
		return "", err
	}

	// Get values from ConfigMap if exist
	if err := strvals.ParseInto(repoInfo.Values, vals); err != nil {
		return "", errors.Wrap(err, "failed parsing --set data")
	}

	// Overwrite using existing values
	existVals, err := k.GetChartValues(repoInfo)
	if err != nil {
		return "", errors.Wrap(err, "failed getting existing values --set data")
	}
	for k, v := range existVals {
		vals[k] = v
	}

	// Check chart dependencies to make sure all are present in /charts
	chartRequested, err := loader.Load(cp)
	if err != nil {
		return "", err
	}

	validInstallableChart, err := isChartInstallable(chartRequested)
	if !validInstallableChart {
		return "", err
	}

	client.Namespace = repoInfo.Namespace

	release, err := client.Run(repoInfo.ReleaseName, chartRequested, vals)
	if err != nil {
		return "", err
	}
	return release.Manifest, nil
}

// GetChartValues gets existing helm values for current installed release
func (k *K8s) GetChartValues(repoInfo *scheduler.HelmRepo) (map[string]interface{}, error) {
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), repoInfo.Namespace, os.Getenv("HELM_DRIVER"), debug); err != nil {
		return nil, err
	}

	client := action.NewGetValues(actionConfig)
	values, err := client.Run(repoInfo.ReleaseName)
	if err != nil {
		return nil, err
	}

	return values, nil
}

// UnInstallHelmChart will uninstall the release
func (k *K8s) UnInstallHelmChart(repoInfo *scheduler.HelmRepo) ([]interface{}, error) {
	var err error
	actionConfig := new(action.Configuration)
	if err = actionConfig.Init(settings.RESTClientGetter(), repoInfo.Namespace, os.Getenv("HELM_DRIVER"), debug); err != nil {
		return nil, err
	}

	client := action.NewUninstall(actionConfig)
	client.Timeout = deleteTasksWaitTimeout
	response, err := client.Run(repoInfo.ReleaseName)
	if err != nil {
		return nil, err
	}

	// Parse the manifest which is a yaml to get the k8s spec objects
	var yamlBuf bytes.Buffer
	yamlBuf.WriteString(response.Release.Manifest)
	specs, err := k.ParseSpecsFromYamlBuf(&yamlBuf)
	if err != nil {
		return nil, err
	}

	return specs, nil
}

func debug(format string, v ...interface{}) {
	format = fmt.Sprintf(" %s\n", format)
	logrus.Debugf(fmt.Sprintf(format, v...))
}
