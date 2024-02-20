package utilities

import (
	"context"
	"fmt"
	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/log"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"math/rand"
	"strconv"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	. "github.com/portworx/torpedo/drivers/applications/apptypes"
	"github.com/portworx/torpedo/drivers/scheduler"
	corev1 "k8s.io/api/core/v1"
)

type AppInfo struct {
	StartDataSupport bool
	User             string
	Password         string
	Port             int
	NodePort         int
	DBName           string
	Hostname         string
	AppType          string
	Namespace        string
	IPAddress        string
}

const (
	svcAnnotationKey                = "startDataSupported"
	userAnnotationKey               = "username"
	passwordAnnotationKey           = "password"
	databaseAnnotationKey           = "databaseName"
	portAnnotationKey               = "port"
	defaultFilePath                 = "/home/cirros/"
	appTypeAnnotationKey            = "appType"
	defaultCmdTimeout               = 20 * time.Second
	defaultCmdRetryInterval         = 5 * time.Second
	defaultKubeconfigMapForKubevirt = "kubevirt-creds"
)

// RandomString generates a random lowercase string of length characters.
func RandomString(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	randomBytes := make([]byte, length)
	for i := range randomBytes {
		randomBytes[i] = letters[rand.Intn(len(letters))]
	}
	randomString := string(randomBytes)
	return randomString
}

// GenerateRandomSQLCommands generates pairs of INSERT, UPDATE, SELECT and DELETE queries for a database
func GenerateRandomSQLCommands(count int, appType string) map[string][]string {
	var randomSqlCommands = make(map[string][]string)
	var tableName string
	var insertCommands []string
	var selectCommands []string
	var deleteCommands []string
	var updateCommands []string

	if appType == Postgres {
		tableName = "pg_validation_" + RandomString(5)
		insertCommands = append(insertCommands, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		key varchar(45) NOT NULL,
		value varchar(45) NOT NULL
	  )`, tableName))
	} else if appType == MySql {
		tableName = "mysql_validation_" + RandomString(5)
		insertCommands = append(insertCommands, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			`+"`key` "+`VARCHAR(45) NOT NULL ,
			value VARCHAR(255)
		  )`, tableName))
	}

	for counter := 0; counter < count; counter++ {
		currentCounter := strconv.Itoa(counter)
		randomValue := "Value-" + RandomString(10)
		updatedRandomValue := "Value-Updated-" + RandomString(10)
		insertCommands = append(insertCommands, fmt.Sprintf("INSERT INTO %s VALUES('%s', '%s')", tableName, currentCounter, randomValue))
		if appType == Postgres {
			selectCommands = append(selectCommands, fmt.Sprintf("SELECT * FROM %s WHERE key='%s'", tableName, currentCounter))
			updateCommands = append(updateCommands, fmt.Sprintf("UPDATE %s SET value='%s' WHERE key='%s'", tableName, updatedRandomValue, currentCounter))
			deleteCommands = append(deleteCommands, fmt.Sprintf("DELETE FROM %s WHERE key='%s'", tableName, currentCounter))
		} else if appType == MySql {
			selectCommands = append(selectCommands, fmt.Sprintf("SELECT * FROM %s WHERE `key`='%s'", tableName, currentCounter))
			updateCommands = append(updateCommands, fmt.Sprintf("UPDATE %s SET value='%s' WHERE `key`='%s'", tableName, updatedRandomValue, currentCounter))
			deleteCommands = append(deleteCommands, fmt.Sprintf("DELETE FROM %s WHERE `key`='%s'", tableName, currentCounter))
		}

	}

	randomSqlCommands["insert"] = insertCommands
	randomSqlCommands["select"] = selectCommands
	randomSqlCommands["update"] = updateCommands
	randomSqlCommands["delete"] = deleteCommands

	// log.Infof("Insert Queries - [%v]", insertCommands)
	// log.Infof("Select Queries - [%v]", selectCommands)
	// log.Infof("Update Queries - [%v]", updateCommands)
	// log.Infof("Delete Queries - [%v]", deleteCommands)

	return randomSqlCommands

}

// GenerateSQLCommandPair generates pairs of INSERT and SELECT queries for a database
func GenerateSQLCommandPair(tableName string, appType string) map[string][]string {
	var sqlCommandMap = make(map[string][]string)
	var selectQuery string
	randomKey := "key-" + RandomString(10)
	randomValue := "value-" + RandomString(10)

	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES('%s', '%s')", tableName, randomKey, randomValue)
	if appType == Postgres {
		selectQuery = fmt.Sprintf("SELECT * FROM %s WHERE key='%s'", tableName, randomKey)
	} else if appType == MySql {
		selectQuery = fmt.Sprintf("SELECT * FROM %s WHERE `key`='%s'", tableName, randomKey)
	}

	sqlCommandMap["insert"] = append(sqlCommandMap["insert"], insertQuery)
	sqlCommandMap["select"] = append(sqlCommandMap["select"], selectQuery)

	return sqlCommandMap
}

// CreateHostNameForApp creates a hostname using service name and namespace
func CreateHostNameForApp(serviceName string, nodePort int32, namespace string) (string, error) {
	var hostname string

	if nodePort != 0 {
		k8sCore := core.Instance()
		nodes, err := k8sCore.GetNodes()
		if err != nil {
			return "", err
		}
		// hostname = nodes.Items[0].Name
		hostname = nodes.Items[0].Status.Addresses[0].Address
	} else {
		hostname = fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace)
	}

	return hostname, nil
}

// ExtractConnectionInfo Extracts the connection information from the service yaml
func ExtractConnectionInfo(ctx *scheduler.Context, context context.Context) (AppInfo, error) {

	// TODO: This needs to be enhanced to support multiple application in one ctx
	var appInfo AppInfo

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*kubevirtv1.VirtualMachine); ok {
			k8sKubevirt := kubevirt.Instance()
			appInfo.Namespace = obj.Namespace
			log.Infof("%+v", obj.Annotations)
			vmInstance, err := k8sKubevirt.GetVirtualMachineInstance(context, obj.Name, obj.Namespace)
			if err != nil {
				return appInfo, err
			}
			if svcAnnotationValue, ok := obj.Annotations[svcAnnotationKey]; ok {
				appInfo.StartDataSupport = svcAnnotationValue == "true"
				if !appInfo.StartDataSupport {
					break
				}
			} else {
				appInfo.StartDataSupport = false
				break
			}
			if userAnnotationValue, ok := obj.Annotations[userAnnotationKey]; ok {
				appInfo.User = userAnnotationValue
			} else {
				return appInfo, fmt.Errorf("Username not found")
			}
			if appTypeAnnotationValue, ok := obj.Annotations[appTypeAnnotationKey]; ok {
				appInfo.AppType = appTypeAnnotationValue
			} else {
				return appInfo, fmt.Errorf("AppType not found")
			}
			appInfo.Hostname = obj.Name

			appInfo.IPAddress = vmInstance.Status.Interfaces[0].IP
			cm, err := core.Instance().GetConfigMap(defaultKubeconfigMapForKubevirt, "default")
			if err != nil {
				return appInfo, err
			}
			appInfo.Password = cm.Data[obj.Name]
			return appInfo, nil

		}
		if obj, ok := specObj.(*corev1.Service); ok {
			appInfo.Namespace = obj.Namespace
			// TODO: This needs to be fetched from spec once CloneAppContextAndTransformWithMappings is fixed
			svc, err := core.Instance().GetService(obj.Name, obj.Namespace)
			if err != nil {
				return appInfo, err
			}
			nodePort := svc.Spec.Ports[0].NodePort
			hostname, err := CreateHostNameForApp(obj.Name, nodePort, obj.Namespace)
			if err != nil {
				return appInfo, fmt.Errorf("Some error occurred while generating hostname")
			}
			appInfo.Hostname = hostname
			appInfo.NodePort = int(nodePort)
			if svcAnnotationValue, ok := obj.Annotations[svcAnnotationKey]; ok {
				appInfo.StartDataSupport = svcAnnotationValue == "true"
				if !appInfo.StartDataSupport {
					continue
				}
			} else {
				appInfo.StartDataSupport = false
				continue
			}
			if userAnnotationValue, ok := obj.Annotations[userAnnotationKey]; ok {
				appInfo.User = userAnnotationValue
			} else {
				return appInfo, fmt.Errorf("Username not found")
			}
			if appTypeAnnotationValue, ok := obj.Annotations[appTypeAnnotationKey]; ok {
				appInfo.AppType = appTypeAnnotationValue
			} else {
				return appInfo, fmt.Errorf("AppType not found")
			}
			if passwordAnnotationValue, ok := obj.Annotations[passwordAnnotationKey]; ok {
				appInfo.Password = passwordAnnotationValue
			} else {
				return appInfo, fmt.Errorf("Password not found")
			}
			if portAnnotationValue, ok := obj.Annotations[portAnnotationKey]; ok {
				appInfo.Port, _ = strconv.Atoi(portAnnotationValue)
			}
			if databaseAnnotationValue, ok := obj.Annotations[databaseAnnotationKey]; ok {
				appInfo.DBName = databaseAnnotationValue
			}
		}
	}

	return appInfo, nil
}

// RunCmdGetOutputOnNode runs the command on a particular node and returns output
func RunCmdGetOutputOnNode(cmd string, n node.Node, nodeDriver node.Driver) (string, error) {
	output, err := nodeDriver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCmdTimeout,
		TimeBeforeRetry: defaultCmdRetryInterval,
		Sudo:            true,
	})
	if err != nil {
		log.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}
	return output, err
}

// GenerateRandomCommandToCreateFiles creates random textfiles with random data
func GenerateRandomCommandToCreateFiles(count int) map[string][]string {
	var randomFileCommands = make(map[string][]string)
	var filePath = defaultFilePath + RandomString(10)
	var insertCommands []string
	var selectCommands []string
	var deleteCommands []string
	var updateCommands []string

	// Generating command to create the dir to hold files if not exists
	createDir := fmt.Sprintf("mkdir -p -m777 %s", filePath)
	log.Infof("Command to create Dir - [%s]", createDir)
	insertCommands = append(insertCommands, createDir)

	for counter := 0; counter < count; counter++ {
		currentCounter := strconv.Itoa(counter)
		fileName := fmt.Sprintf("%s/%s_%s.txt", filePath, currentCounter, RandomString(4))
		// fileContent := fmt.Sprintf("%s", RandomString(10))
		insertCommands = append(insertCommands, fmt.Sprintf("touch %s", fileName))
		selectCommands = append(selectCommands, fmt.Sprintf("ls %s", fileName))
		updateCommands = append(updateCommands, fmt.Sprintf("echo '%s' >> %s", RandomString(5), fileName))
		deleteCommands = append(deleteCommands, fmt.Sprintf("rm %s", fileName))
	}

	randomFileCommands["insert"] = insertCommands
	randomFileCommands["select"] = selectCommands
	randomFileCommands["update"] = updateCommands
	randomFileCommands["delete"] = deleteCommands

	return randomFileCommands
}
