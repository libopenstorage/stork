package applications

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	. "github.com/portworx/torpedo/drivers/applications/apptypes"
	. "github.com/portworx/torpedo/drivers/utilities"
	"github.com/portworx/torpedo/pkg/log"
)

type PostgresConfig struct {
	Hostname    string
	User        string
	Password    string
	Port        int
	NodePort    int
	DBName      string
	Namespace   string
	SQLCommands map[string]map[string][]string
}

// GetConnection returns a connection object for postgres database
func (app *PostgresConfig) GetConnection(ctx context.Context) (*pgx.Conn, error) {

	if app.Port == 0 {
		app.Port = app.DefaultPort()
	}

	if app.DBName == "" {
		app.DBName = app.DefaultDBName()
	}

	var url string

	if app.NodePort != 0 {
		// Connect with NodePort Service
		url = fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			app.User, app.Password, app.Hostname, app.NodePort, app.DBName)
	} else {
		// Connect with Cluster Service
		url = fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			app.User, app.Password, app.Hostname, app.Port, app.DBName)
	}

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %s, Conn String - [%s]", err, url)
	}

	return conn, nil
}

// DefaultPort returns default port for postgres
func (app *PostgresConfig) DefaultPort() int { return 5432 }

// DefaultDBName returns default database name
func (app *PostgresConfig) DefaultDBName() string { return "postgres" }

// ExecuteCommand executes a SQL command for postgres database
func (app *PostgresConfig) ExecuteCommand(commands []string, ctx context.Context) error {

	conn, err := app.GetConnection(ctx)
	if err != nil {
		return err
	}

	defer conn.Close(ctx)

	for _, eachCommand := range commands {
		_, err = conn.Exec(ctx, eachCommand)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertBackupData inserts the rows generated initially by utilities or rows passed
func (app *PostgresConfig) InsertBackupData(ctx context.Context, identifier string, commnads []string) error {

	var err error
	log.InfoD("Inserting data")
	if len(commnads) == 0 {
		log.Infof("Inserting below data : %s", strings.Join(app.SQLCommands[identifier]["insert"], "\n"))
		err = app.ExecuteCommand(app.SQLCommands[identifier]["insert"], ctx)
	} else {
		log.Infof("Inserting below data : %s", strings.Join(commnads, "\n"))
		err = app.ExecuteCommand(commnads, ctx)
	}

	return err
}

// Return data inserted before backup
func (app *PostgresConfig) GetBackupData(identifier string) []string {
	if _, ok := app.SQLCommands[identifier]; ok {
		return app.SQLCommands[identifier]["select"]
	} else {
		log.InfoD("%s not found in app sql command", identifier)
		log.Infof("All current SQL commands - %+v", app.SQLCommands)
		return nil
	}
}

// CheckDataPresent checks if the mentioned entry is present or not in the database
func (app *PostgresConfig) CheckDataPresent(selectQueries []string, ctx context.Context) error {

	log.InfoD("Running Select Queries")

	conn, err := app.GetConnection(ctx)

	if err != nil {
		return err
	}

	defer conn.Close(ctx)

	var key string
	var value string
	var queryNotFoundList []string

	for _, eachQuery := range selectQueries {
		currentRow := conn.QueryRow(ctx, eachQuery)
		err := currentRow.Scan(&key, &value)

		if err != nil {
			log.InfoD("Select query failed - [%s] Error - [%s]", eachQuery, err.Error())
			queryNotFoundList = append(queryNotFoundList, eachQuery)
		}
	}

	if len(queryNotFoundList) != 0 {
		errorMessage := strings.Join(queryNotFoundList, "\n")
		return fmt.Errorf("Below results not found in the table:\n %s", errorMessage)
	}
	return nil
}

// UpdateBackupData updates the rows generated initially by utilities
func (app *PostgresConfig) UpdateBackupData(ctx context.Context, identifier string) error {

	log.InfoD("Running Update Queries")
	err := app.ExecuteCommand(app.SQLCommands[identifier]["update"], ctx)

	return err
}

// DeleteBackupData deletes the rows generated initially by utilities
func (app *PostgresConfig) DeleteBackupData(ctx context.Context, identifier string) error {

	log.InfoD("Running Delete Queries")
	err := app.ExecuteCommand(app.SQLCommands[identifier]["delete"], ctx)

	return err
}

// StartData - Go routine to run parallal with app to keep injecting data every 2 seconds
func (app *PostgresConfig) StartData(command <-chan string, ctx context.Context) error {
	var status = DataStart
	var allSelectCommands []string
	var allErrors []string
	var tableName = "table_" + RandomString(4)

	createTableQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		key varchar(45) NOT NULL,
		value varchar(45) NOT NULL
	  )`, tableName)
	err := app.ExecuteCommand([]string{createTableQuery}, ctx)
	if err != nil {
		allErrors = append(allErrors, err.Error())
	}
	for {
		select {
		case cmd := <-command:
			switch cmd {
			case DataStop:
				if len(allErrors) != 0 {
					return fmt.Errorf(strings.Join(allErrors, "\n"))
				}
				err := app.CheckDataPresent(allSelectCommands, ctx)
				return err

			case DataPause:
				status = DataPause
			default:
				status = DataStart
			}
		default:
			if status == DataStart {
				commandPair, err := app.startInsertingData(tableName, ctx)
				if err != nil {
					allErrors = append(allErrors, err.Error())
				}
				allSelectCommands = append(allSelectCommands, commandPair["select"]...)
				time.Sleep(2 * time.Second)
			}
		}
	}
}

// startInsertingData is helper to insert generate rows and insert data parallely for postgres app
func (app *PostgresConfig) startInsertingData(tableName string, ctx context.Context) (map[string][]string, error) {

	commandPair := GenerateSQLCommandPair(tableName, Postgres)

	err := app.ExecuteCommand(commandPair["insert"], ctx)
	if err != nil {
		return commandPair, err
	}

	return commandPair, nil
}

// Update the existing SQL commands
func (app *PostgresConfig) UpdateDataCommands(count int, identifier string) {
	app.SQLCommands[identifier] = GenerateRandomSQLCommands(count, Postgres)
	log.InfoD("SQL Commands updated")
}

// Update the existing SQL commands
func (app *PostgresConfig) AddDataCommands(identifier string, commands map[string][]string) {
	app.SQLCommands[identifier] = commands
	log.InfoD("Sql commands added")
}

// Generate and return random SQL commands
func (app *PostgresConfig) GetRandomDataCommands(count int) map[string][]string {
	return GenerateRandomSQLCommands(count, Postgres)
}

// Get the application type
func (app *PostgresConfig) GetApplicationType() string {
	return Postgres
}

// Get Namespace of the app
func (app *PostgresConfig) GetNamespace() string {
	return app.Namespace
}
