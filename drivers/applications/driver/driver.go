package driver

import (
	"context"

	. "github.com/portworx/torpedo/drivers/applications/apptypes"
	. "github.com/portworx/torpedo/drivers/applications/mysql"
	. "github.com/portworx/torpedo/drivers/applications/postgres"
	. "github.com/portworx/torpedo/drivers/utilities"
)

type ApplicationDriver interface {
	// DefaultPort returns the default port for the application
	DefaultPort() int

	// DefaultDBName returns default database name in case of an database app
	DefaultDBName() string

	// ExecuteCommand executes a command on the application
	ExecuteCommand(commands []string, ctx context.Context) error

	// StartData starts injecting continous data to the application
	StartData(command <-chan string, ctx context.Context) error

	// CheckDataPresent checks if the passed data is present in the app or not
	CheckDataPresent(selectQueries []string, ctx context.Context) error

	// UpdateDataCommands updates the SQL queries/data injection commands for the app
	UpdateDataCommands(count int, identifier string)

	// InsertBackupData inserts data before and after backup
	InsertBackupData(ctx context.Context, identifier string, commands []string) error

	// GetBackupData gets the sql queries inserted before or after backup
	GetBackupData(identifier string) []string

	// GetRandomDataCommands creates random CRUD queries for an application
	GetRandomDataCommands(count int) map[string][]string

	// AddDataCommands adds CRUD queries to the application struct
	AddDataCommands(identifier string, commands map[string][]string)

	// GetApplicationType retruns the application type
	GetApplicationType() string

	// GetNamespace returns the application namespace
	GetNamespace() string
}

// GetApplicationDriver returns struct of appType provided as input
func GetApplicationDriver(appType string, hostname string, user string,
	password string, port int, dbname string, nodePort int, namespace string) (ApplicationDriver, error) {

	switch appType {
	case Postgres:
		return &PostgresConfig{
			Hostname: hostname,
			User:     user,
			Password: password,
			Port:     port,
			DBName:   dbname,
			SQLCommands: map[string]map[string][]string{
				"default": GenerateRandomSQLCommands(20, appType),
			},
			NodePort:  nodePort,
			Namespace: namespace,
		}, nil
	case MySql:
		return &MySqlConfig{
			Hostname: hostname,
			User:     user,
			Password: password,
			Port:     port,
			DBName:   dbname,
			SQLCommands: map[string]map[string][]string{
				"default": GenerateRandomSQLCommands(20, appType),
			},
			NodePort:  nodePort,
			Namespace: namespace,
		}, nil
	default:
		return &PostgresConfig{
			Hostname: hostname,
			User:     user,
			Password: password,
			Port:     port,
			DBName:   dbname,
			SQLCommands: map[string]map[string][]string{
				"default": GenerateRandomSQLCommands(20, appType),
			},
			NodePort:  nodePort,
			Namespace: namespace,
		}, nil

	}
}
