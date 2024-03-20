package tests

import (
	context1 "context"
	"fmt"
	"strings"

	"math/rand"
	"time"

	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"

	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo/v2"
)

// All Longevity types for backup

// Global variables to be used by all flows
var (
	LongevityBackupLocationName      string
	LongevityBackupLocationUID       string
	LongevityLockedBackupLocationMap map[string]string
	LongevityAllNamespaces           []string
	LongevityClusterUID              string
	LongevityScheduledAppContexts    []*scheduler.Context
	LongevityAllBackupNames          []string
	LongevityAllLockedBackupNames    []string
)

type PxBackupLongevity struct {
	CustomData      *CustomData
	ApplicationData *ApplicationData
	BackupData      *BackupData
	RestoreData     *RestoreData
}

type CustomData struct {
	Integers map[string]int
	Strings  map[string]string
}

type BackupData struct {
	Namespaces              []string
	BackupLocationName      string
	BackupLocationUID       string
	LockedBackupLocationMap map[string]string
	ClusterUid              string
	BackupName              string
}

type RestoreData struct {
	RestoreMap  map[string]string
	RestoreName string
}

type ApplicationData struct {
	SchedulerContext []*scheduler.Context
}

type EventData struct {
	SchedulerContext        []*scheduler.Context
	AppContext              context1.Context
	BackupNamespaces        []string
	BackupLocationName      string
	BackupLocationUID       string
	LockedBackupLocationMap map[string]string
	ClusterUid              string
	BackupNames             []string
	LockedBackupNames       []string
	RestoreName             string
}

type EventBuilderResponse struct {
	Error              error
	TimeTakenInMinutes float32
	HighlightEvent     string
	EventData          EventData
}

type EventResponse struct {
	Name               string
	EventBuilders      map[string]*EventBuilderResponse
	Errors             []error
	TimeTakenInMinutes float32
	HighlightEvents    []string
	DisruptiveEventRan []string
	Status             bool
}

const (
	EventScheduleApps                               = "EventScheduleApps"
	EventValidateScheduleApplication                = "EventValidateScheduleApplication"
	EventAddCredentialandBackupLocation             = "EventAddCredentialandBackupLocation"
	EventAddSourceAndDestinationCluster             = "EventAddSourceAndDestinationCluster"
	EventAddLockedBucketCredentialandBackupLocation = "EventAddLockedBucketCredentialandBackupLocation"
	EventCreateBackup                               = "EventCreateBackup"
	EventCreateLockedBackup                         = "EventCreateLockedBackup"
	EventRestore                                    = "EventRestore"
)

var AllBuilders = map[string]PxBackupEventBuilder{
	EventScheduleApps:                               eventScheduleApps,
	EventValidateScheduleApplication:                eventValidateScheduleApplication,
	EventAddCredentialandBackupLocation:             eventAddCredentialandBackupLocation,
	EventAddSourceAndDestinationCluster:             eventAddSourceAndDestinationCluster,
	EventAddLockedBucketCredentialandBackupLocation: eventAddLockedBucketCredentialandBackupLocation,
	EventCreateBackup:                               eventCreateBackup,
	EventCreateLockedBackup:                         eventCreateLockedBackup,
	EventRestore:                                    eventRestore,
}

type PxBackupEventBuilder func(*PxBackupLongevity) (error, string, EventData)

func GetLongevityInputParams() PxBackupLongevity {

	var customData = CustomData{
		Integers: make(map[string]int),
		Strings:  make(map[string]string),
	}

	var backupData = BackupData{
		Namespaces:         make([]string, 0),
		BackupLocationName: "",
		BackupLocationUID:  "",
		ClusterUid:         "",
	}

	var restoreData = RestoreData{
		RestoreMap:  make(map[string]string),
		RestoreName: "",
	}

	var applicationData = ApplicationData{
		SchedulerContext: make([]*scheduler.Context, 0),
	}

	var longevityStruct = PxBackupLongevity{
		CustomData:      &customData,
		ApplicationData: &applicationData,
		BackupData:      &backupData,
		RestoreData:     &restoreData,
	}

	return longevityStruct
}

func GetLongevityEventResponse() EventResponse {
	var someOtherVar = make(map[string]*EventBuilderResponse)

	var eventResponse = EventResponse{
		EventBuilders: someOtherVar,
	}

	return eventResponse
}

// GetRandomNamespacesForBackup returns random namespace(s) for backup
func GetRandomNamespacesForBackup() []string {
	var allNamespacesForBackupMap = make(map[string]bool)
	var allNamepsacesForBackup []string
	rand.Seed(time.Now().Unix()) // initialize global pseudo random generator

	numberOfNamespaces := rand.Intn(len(LongevityAllNamespaces))

	for i := 0; i <= numberOfNamespaces; i++ {
		allNamespacesForBackupMap[LongevityAllNamespaces[rand.Intn(len(LongevityAllNamespaces))]] = true
	}

	for namespaceName := range allNamespacesForBackupMap {
		allNamepsacesForBackup = append(allNamepsacesForBackup, namespaceName)
	}

	log.Infof("Namespaces selected for restore - [%v]", allNamepsacesForBackup)

	return allNamepsacesForBackup
}

func GetRandomBackupForRestore() string {
	rand.Seed(time.Now().Unix()) // initialize global pseudo random generator

	return LongevityAllBackupNames[rand.Intn(len(LongevityAllBackupNames))]
}

func UpdateEventResponse(eventResponse *EventResponse) {
	for _, builderResponse := range eventResponse.EventBuilders {
		eventResponse.TimeTakenInMinutes += builderResponse.TimeTakenInMinutes
		if builderResponse.Error != nil {
			eventResponse.Errors = append(eventResponse.Errors, builderResponse.Error)
		}
		eventResponse.HighlightEvents = append(eventResponse.HighlightEvents, builderResponse.HighlightEvent)
	}
	if eventResponse.Errors != nil {
		eventResponse.Status = false
	} else {
		eventResponse.Status = true
	}
}

// All longevity events

// Event to schedule apps on cluster
func eventScheduleApps(inputsForEventBuilder *PxBackupLongevity) (error, string, EventData) {
	defer GinkgoRecover()
	eventData := &EventData{}

	var scheduledAppContexts = make([]*scheduler.Context, 0)
	var bkpNamespaces = make([]string, 0)

	for i := 0; i < Inst().GlobalScaleFactor; i++ {
		taskName := fmt.Sprintf("%s-%d-%s", TaskNamePrefix, i, RandomString(5))
		appContexts := ScheduleApplications(taskName)
		for _, ctx := range appContexts {
			ctx.ReadinessTimeout = AppReadinessTimeout
			namespace := GetAppNamespace(ctx, taskName)
			bkpNamespaces = append(bkpNamespaces, namespace)
			scheduledAppContexts = append(scheduledAppContexts, ctx)
		}
	}

	eventData.BackupNamespaces = bkpNamespaces
	eventData.SchedulerContext = scheduledAppContexts

	return nil, "", *eventData
}

// Event to validate app but later will be changed to create handler and start data for app
func eventValidateScheduleApplication(inputsForEventBuilder *PxBackupLongevity) (error, string, EventData) {
	defer GinkgoRecover()
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
	eventData := &EventData{}
	_, _ = ValidateApplicationsStartData(inputsForEventBuilder.ApplicationData.SchedulerContext, ctx)
	return nil, "", *eventData
}

// Event for backup location and cred add
func eventAddCredentialandBackupLocation(inputsForEventBuilder *PxBackupLongevity) (error, string, EventData) {
	defer GinkgoRecover()
	eventData := &EventData{}
	backupLocationMap := make(map[string]string)
	var cloudCredUidList []string
	var providers = GetBackupProviders()
	var backupLocationUID string

	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")

	log.InfoD("Creating cloud credentials and backup location")
	for _, provider := range providers {
		cloudCredUID := uuid.New()
		cloudCredUidList = append(cloudCredUidList, cloudCredUID)
		backupLocationUID = uuid.New()
		credName := fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
		err := CreateCloudCredential(provider, credName, cloudCredUID, BackupOrgID, ctx)
		if err != nil {
			return err, "", *eventData
		}
		log.InfoD("Created Cloud Credentials with name - %s", credName)
		customBackupLocationName := fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
		err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
		if err != nil {
			return err, "", *eventData
		}
		backupLocationMap[backupLocationUID] = customBackupLocationName
		log.InfoD("Created Backup Location with name - %s", customBackupLocationName)

		eventData.BackupLocationName = customBackupLocationName
		eventData.BackupLocationUID = backupLocationUID
	}

	return nil, "", *eventData
}

// Event for backup location and cred add
func eventAddLockedBucketCredentialandBackupLocation(inputsForEventBuilder *PxBackupLongevity) (error, string, EventData) {
	defer GinkgoRecover()
	eventData := &EventData{}
	eventData.LockedBackupLocationMap = make(map[string]string)
	var providers = GetBackupProviders()
	var backupLocationUID string

	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return err, "", *eventData
	}
	modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
	log.InfoD("Creating cloud credentials and backup location")
	for _, provider := range providers {
		for _, mode := range modes {
			cloudCredUID := uuid.New()
			credName := fmt.Sprintf("autogenerated-locked-cred-%v", time.Now().Unix())
			err := CreateCloudCredential(provider, credName, cloudCredUID, BackupOrgID, ctx)
			if err != nil {
				return err, "", *eventData
			}
			bucketName := fmt.Sprintf("%s-%s-%v", "longevity-locked", strings.ToLower(mode), time.Now().Unix())
			err = CreateS3Bucket(bucketName, true, 3, mode)
			if err != nil {
				return err, "", *eventData
			}
			log.Infof("Bucket created with name - %s", bucketName)
			backupLocationUID = uuid.New()
			customBackupLocationName := fmt.Sprintf("%s-%s-lock-%v", "autogenerated", strings.ToLower(mode), time.Now().Unix())
			err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, bucketName, BackupOrgID, "", true)
			if err != nil {
				return err, "", *eventData
			}
			eventData.LockedBackupLocationMap[customBackupLocationName] = backupLocationUID
			log.InfoD("Created Locked Backup Location with name - %s", customBackupLocationName)
		}
	}

	return nil, "", *eventData
}

// Event for Source and Dest Cluster add
func eventAddSourceAndDestinationCluster(inputsForEventBuilder *PxBackupLongevity) (error, string, EventData) {
	defer GinkgoRecover()
	eventData := &EventData{}
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")

	log.Infof("Adding Clusters for backup")
	err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
	if err != nil {
		return err, "", *eventData
	}
	clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
	if err != nil {
		return err, "", *eventData
	}
	if clusterStatus != api.ClusterInfo_StatusInfo_Online {
		return fmt.Errorf("Cluster %s is not online. Cluster Status: [%s]", SourceClusterName, clusterStatus), fmt.Sprintf("Cluster added but not online"), *eventData
	}

	clusterUid, err := Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
	if err != nil {
		return err, "", *eventData
	}
	eventData.ClusterUid = clusterUid

	return nil, "", *eventData
}

// Event for Backup Creation
func eventCreateBackup(inputsForEventBuilder *PxBackupLongevity) (error, string, EventData) {
	defer GinkgoRecover()

	eventData := &EventData{}
	var backupNames []string
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")

	log.Infof("Creating a manual backup")
	for _, namespace := range inputsForEventBuilder.BackupData.Namespaces {
		backupName := fmt.Sprintf("%s-%v-%s", BackupNamePrefix, time.Now().Unix(), RandomString(10))
		labelSelectors := make(map[string]string)
		appContextsToBackup := FilterAppContextsByNamespace(inputsForEventBuilder.ApplicationData.SchedulerContext, []string{namespace})
		err := CreateBackupWithValidation(
			ctx,
			backupName,
			SourceClusterName,
			inputsForEventBuilder.BackupData.BackupLocationName,
			inputsForEventBuilder.BackupData.BackupLocationUID,
			appContextsToBackup,
			labelSelectors,
			BackupOrgID,
			inputsForEventBuilder.BackupData.ClusterUid, "", "", "", "")
		if err != nil {
			return err, "Error occurred while taking backup", *eventData
		}
		backupNames = append(backupNames, backupName)
	}

	eventData.BackupNames = backupNames
	LongevityAllBackupNames = append(LongevityAllBackupNames, backupNames...)

	return nil, "", *eventData
}

// Event for Backup Creation
func eventCreateLockedBackup(inputsForEventBuilder *PxBackupLongevity) (error, string, EventData) {
	defer GinkgoRecover()

	eventData := &EventData{}
	var backupNames []string
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return err, "", *eventData
	}

	log.Infof("Creating a manual locked backup")
	for _, namespace := range inputsForEventBuilder.BackupData.Namespaces {
		for lockedBackupLocationName, lockedBackupLocationUid := range inputsForEventBuilder.BackupData.LockedBackupLocationMap {
			backupName := fmt.Sprintf("%s-%v-%s", BackupNamePrefix, time.Now().Unix(), RandomString(10))
			labelSelectors := make(map[string]string)
			appContextsToBackup := FilterAppContextsByNamespace(inputsForEventBuilder.ApplicationData.SchedulerContext, []string{namespace})
			err := CreateBackupWithValidation(
				ctx,
				backupName,
				SourceClusterName,
				lockedBackupLocationName,
				lockedBackupLocationUid,
				appContextsToBackup,
				labelSelectors,
				BackupOrgID,
				inputsForEventBuilder.BackupData.ClusterUid, "", "", "", "")
			if err != nil {
				return err, "Error occurred while taking backup", *eventData
			}
			backupNames = append(backupNames, backupName)
		}
	}

	eventData.BackupNames = backupNames
	LongevityAllLockedBackupNames = append(LongevityAllLockedBackupNames, backupNames...)

	return nil, "", *eventData
}

func eventRestore(inputsForEventBuilder *PxBackupLongevity) (err error, restoreName string, eventData EventData) {
	defer func() {
		log.InfoD("switching to default context")
		errSetContext := SetClusterContext("")
		if errSetContext != nil {
			if err == nil {
				err = errSetContext
			}
			log.Error("failed to SetClusterContext to default cluster", errSetContext)
		}
	}()

	ctx, errGetCtx := backup.GetAdminCtxFromSecret()
	if errGetCtx != nil {
		err := errGetCtx
		log.Error("Fetching px-central-admin ctx", err)
		return err, "", eventData
	}

	restoreName = fmt.Sprintf("%s-%s-%s", RestoreNamePrefix, inputsForEventBuilder.BackupData.BackupName, RandomString(5))
	appContextsExpectedInBackup := FilterAppContextsByNamespace(inputsForEventBuilder.ApplicationData.SchedulerContext, inputsForEventBuilder.BackupData.Namespaces)
	err = CreateRestoreWithValidation(ctx, restoreName, inputsForEventBuilder.BackupData.BackupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
	if err != nil {
		return err, fmt.Sprintf("Restore failed for %s", restoreName), eventData
	}

	eventData.RestoreName = restoreName

	return err, "", eventData
}

func RunBuilder(eventBuilderName string, inputsForEventBuilder *PxBackupLongevity, eventResponse *EventResponse) EventData {
	defer GinkgoRecover()
	eventBuilder := AllBuilders[eventBuilderName]
	eventBuilderIdentifier := eventBuilderName + "-" + time.Now().Format("15:04:05.000")
	eventResponse.EventBuilders[eventBuilderIdentifier] = &EventBuilderResponse{}

	startTime := time.Now()

	err, highlightEvent, eventData := eventBuilder(inputsForEventBuilder)
	if err != nil {
		eventResponse.EventBuilders[eventBuilderIdentifier].Error = err
	}
	if highlightEvent != "" {
		eventResponse.EventBuilders[eventBuilderIdentifier].HighlightEvent = highlightEvent
	}
	eventResponse.EventBuilders[eventBuilderIdentifier].EventData = eventData
	eventResponse.EventBuilders[eventBuilderIdentifier].TimeTakenInMinutes = float32(time.Since(startTime).Minutes())

	return eventData
}

// Helpers for events
func getGlobalBucketName(provider string) string {
	var bucketName string
	switch provider {
	case drivers.ProviderAws:
		bucketName = fmt.Sprintf("%s-%s", GlobalAWSBucketPrefix, "pxb-ssie")
	case drivers.ProviderAzure:
		bucketName = fmt.Sprintf("%s-%s", GlobalAzureBucketName, "pxb-ssie")
	case drivers.ProviderGke:
		bucketName = fmt.Sprintf("%s-%s", GlobalGCPBucketPrefix, "pxb-ssie")
	case drivers.ProviderNfs:
		bucketName = fmt.Sprintf("%s-%s", GlobalGCPBucketPrefix, "pxb-ssie")
	default:
		bucketName = fmt.Sprintf("%s-%s", "default", "pxb-ssie")
	}
	CreateBucket(bucketName, provider)
	log.Infof("Bucket created with name - %s", GlobalAWSBucketName)
	return bucketName
}

func getGlobalLockedBucketName(provider string) string {
	if provider == drivers.ProviderAws {
		return GlobalAWSLockedBucketName
	} else {
		log.Errorf("environment variable [%s] not provided with valid values", "PROVIDERS")
		return ""
	}
}

// All Longevity Triggers

// Trigger to create cred and bucket for backup
func TriggerAddBackupCredAndBucket(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(SetupBackupBucketAndCreds)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: SetupBackupBucketAndCreds,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	result := GetLongevityEventResponse()
	result.Name = "Add global cloud location for backup"
	inputForBuilder := GetLongevityInputParams()

	eventData := RunBuilder(EventAddCredentialandBackupLocation, &inputForBuilder, &result)

	// Setting global variables for backup
	LongevityBackupLocationName = eventData.BackupLocationName
	LongevityBackupLocationUID = eventData.BackupLocationUID

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

	updateMetrics(*event)

}

// Trigger to create cred and bucket for backup
func TriggerAddLockedBackupCredAndBucket(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(SetupBackupLockedBucketAndCreds)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: SetupBackupLockedBucketAndCreds,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	result := GetLongevityEventResponse()
	result.Name = "Add global cloud location for locked backup"
	inputForBuilder := GetLongevityInputParams()

	eventData := RunBuilder(EventAddLockedBucketCredentialandBackupLocation, &inputForBuilder, &result)

	// Setting global variables for backup
	LongevityLockedBackupLocationMap = eventData.LockedBackupLocationMap

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

	updateMetrics(*event)

}

// Trigger to add a backup cluster
func TriggerAddBackupCluster(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AddBackupCluster)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AddBackupCluster,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	result := GetLongevityEventResponse()
	result.Name = "Add global cloud location for backup"
	inputForBuilder := GetLongevityInputParams()

	eventData := RunBuilder(EventAddSourceAndDestinationCluster, &inputForBuilder, &result)

	// Setting global variables for backup
	LongevityClusterUID = eventData.ClusterUid

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

}

// Trigger to deploy backup apps with or without data validation
func TriggerDeployBackupApps(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(DeployBackupApps)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: DeployBackupApps,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	result := GetLongevityEventResponse()
	result.Name = "Schedule And Validate App"
	inputForBuilder := GetLongevityInputParams()

	eventData := RunBuilder(EventScheduleApps, &inputForBuilder, &result)
	LongevityScheduledAppContexts = append(LongevityScheduledAppContexts, eventData.SchedulerContext...)
	LongevityAllNamespaces = append(LongevityAllNamespaces, eventData.BackupNamespaces...)

	inputForBuilder.ApplicationData.SchedulerContext = eventData.SchedulerContext

	_ = RunBuilder(EventValidateScheduleApplication, &inputForBuilder, &result)

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

}

// Trigger to create backup and validate
func TriggerCreateBackup(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CreatePxBackup)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CreatePxBackup,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	result := GetLongevityEventResponse()
	result.Name = "Create Backup"
	inputForBuilder := GetLongevityInputParams()

	log.Infof("Creating Backup")
	inputForBuilder.BackupData.BackupLocationName = LongevityBackupLocationName
	inputForBuilder.BackupData.BackupLocationUID = LongevityBackupLocationUID
	inputForBuilder.BackupData.ClusterUid = LongevityClusterUID
	inputForBuilder.BackupData.Namespaces = GetRandomNamespacesForBackup()
	inputForBuilder.ApplicationData.SchedulerContext = LongevityScheduledAppContexts

	_ = RunBuilder(EventCreateBackup, &inputForBuilder, &result)

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

}

// Trigger to create backup and validate
func TriggerCreateLockedBackup(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CreatePxLockedBackup)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CreatePxLockedBackup,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	result := GetLongevityEventResponse()
	result.Name = "Create Locked Backup"
	inputForBuilder := GetLongevityInputParams()

	log.Infof("Creating Backup")
	inputForBuilder.BackupData.LockedBackupLocationMap = LongevityLockedBackupLocationMap
	inputForBuilder.BackupData.ClusterUid = LongevityClusterUID
	inputForBuilder.BackupData.Namespaces = GetRandomNamespacesForBackup()
	inputForBuilder.ApplicationData.SchedulerContext = LongevityScheduledAppContexts

	_ = RunBuilder(EventCreateLockedBackup, &inputForBuilder, &result)

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

}

// Trigger to create backup and restore from same backup
func TriggerCreateBackupAndRestore(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CreatePxBackupAndRestore)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CreatePxBackupAndRestore,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	result := GetLongevityEventResponse()
	result.Name = "Create Backup and Restore"
	inputForBuilder := GetLongevityInputParams()

	inputForBuilder.BackupData.BackupLocationName = LongevityBackupLocationName
	inputForBuilder.BackupData.BackupLocationUID = LongevityBackupLocationUID
	inputForBuilder.BackupData.ClusterUid = LongevityClusterUID
	inputForBuilder.BackupData.Namespaces = GetRandomNamespacesForBackup()
	inputForBuilder.ApplicationData.SchedulerContext = LongevityScheduledAppContexts

	eventData := RunBuilder(EventCreateBackup, &inputForBuilder, &result)

	inputForBuilder.BackupData.BackupName = eventData.BackupNames[0]

	_ = RunBuilder(EventRestore, &inputForBuilder, &result)

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

}

func TriggerCreateRandomRestore(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {

	defer GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CreateRandomRestore)

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CreateRandomRestore,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	result := GetLongevityEventResponse()
	result.Name = "Create Restore From Random Backup"
	inputForBuilder := GetLongevityInputParams()

	inputForBuilder.BackupData.BackupName = GetRandomBackupForRestore()
	log.Infof("Creating restore from [%s]", inputForBuilder.BackupData.BackupName)

	_ = RunBuilder(EventRestore, &inputForBuilder, &result)

	UpdateEventResponse(&result)

	for _, err := range result.Errors {
		UpdateOutcome(event, err)
	}

}
