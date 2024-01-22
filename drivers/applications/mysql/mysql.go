package applications

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/portworx/torpedo/drivers/applications/apptypes"
	. "github.com/portworx/torpedo/drivers/utilities"
	"github.com/portworx/torpedo/pkg/log"
)

type MySqlConfig struct {
	Hostname    string
	User        string
	Password    string
	Port        int
	NodePort    int
	DBName      string
	Namespace   string
	SQLCommands map[string]map[string][]string
}

// GetConnection returns a connection object for mysql database
func (app *MySqlConfig) GetConnection(ctx context.Context) (*sql.DB, error) {

	if app.Port == 0 {
		app.Port = app.DefaultPort()
	}

	if app.DBName == "" {
		app.DBName = app.DefaultDBName()
	}

	var url string

	if app.NodePort != 0 {
		// Connect with NodePort Service
		url = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			app.User, app.Password, app.Hostname, app.NodePort, app.DBName)
	} else {
		// Connect with Cluster Service
		url = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			app.User, app.Password, app.Hostname, app.Port, app.DBName)
	}

	conn, err := sql.Open("mysql", url)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %s, Conn String - [%s]", err, url)
	}

	return conn, nil
}

// DefaultPort returns default port for mysql
func (app *MySqlConfig) DefaultPort() int { return 3306 }

// DefaultDBName returns default database name
func (app *MySqlConfig) DefaultDBName() string { return "mysql" }

// ExecuteCommand executes a SQL command for mysql database
func (app *MySqlConfig) ExecuteCommand(commands []string, ctx context.Context) error {

	conn, err := app.GetConnection(ctx)

	if err != nil {
		return err
	}

	defer conn.Close()

	for _, eachCommand := range commands {
		_, err = conn.ExecContext(ctx, eachCommand)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertBackupData inserts the rows generated initially by utilities or rows passed
func (app *MySqlConfig) InsertBackupData(ctx context.Context, identifier string, commnads []string) error {

	var err error
	log.InfoD("Inserting data")
	if len(commnads) == 0 {
		log.InfoD("Inserting below data : %s", strings.Join(app.SQLCommands[identifier]["insert"], "\n"))
		err = app.ExecuteCommand(app.SQLCommands[identifier]["insert"], ctx)
	} else {
		log.InfoD("Inserting below data : %s", strings.Join(commnads, "\n"))
		err = app.ExecuteCommand(commnads, ctx)
	}

	return err
}

// Return data inserted before backup
func (app *MySqlConfig) GetBackupData(identifier string) []string {
	if _, ok := app.SQLCommands[identifier]; ok {
		return app.SQLCommands[identifier]["select"]
	} else {
		return nil
	}
}

// CheckDataPresent checks if the mentioned entry is present or not in the database
func (app *MySqlConfig) CheckDataPresent(selectQueries []string, ctx context.Context) error {

	log.InfoD("Running Select Queries")

	conn, err := app.GetConnection(ctx)
	if err != nil {
		return err
	}

	defer conn.Close()

	var key string
	var value string
	var queryNotFoundList []string

	for _, eachQuery := range selectQueries {
		currentRow := conn.QueryRowContext(ctx, eachQuery)
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
func (app *MySqlConfig) UpdateBackupData(ctx context.Context) error {

	log.InfoD("Running Update Queries")
	err := app.ExecuteCommand(app.SQLCommands["default"]["update"], ctx)

	return err
}

// DeleteBackupData deletes the rows generated initially by utilities
func (app *MySqlConfig) DeleteBackupData(ctx context.Context) error {

	log.InfoD("Running Delete Queries")
	err := app.ExecuteCommand(app.SQLCommands["default"]["delete"], ctx)

	return err
}

// StartData - Go routine to run parallal with app to keep injecting data every 2 seconds
func (app *MySqlConfig) StartData(command <-chan string, ctx context.Context) error {
	var status = DataStart
	var allSelectCommands []string
	var allErrors []string
	var tableName = "table_" + RandomString(4)

	createTableQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		`+"`key` "+`VARCHAR(45) NOT NULL,
		value VARCHAR(255)
	  )`, tableName)
	err := app.ExecuteCommand([]string{createTableQuery}, ctx)
	if err != nil {
		log.InfoD("Error while creating table - [%s]", err.Error())
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

// startInsertingData is helper to insert generate rows and insert data parallely for mysql app
func (app *MySqlConfig) startInsertingData(tableName string, ctx context.Context) (map[string][]string, error) {

	commandPair := GenerateSQLCommandPair(tableName, MySql)

	err := app.ExecuteCommand(commandPair["insert"], ctx)
	if err != nil {
		return commandPair, err
	}

	return commandPair, nil
}

// Update the existing SQL commands
func (app *MySqlConfig) UpdateDataCommands(count int, identifier string) {
	app.SQLCommands[identifier] = GenerateRandomSQLCommands(count, MySql)
	log.InfoD("SQL Commands updated")
}

// Add SQL commands
func (app *MySqlConfig) AddDataCommands(identifier string, commands map[string][]string) {
	app.SQLCommands[identifier] = commands
	log.InfoD("Sql commands added")
}

// Generate and return random SQL commands
func (app *MySqlConfig) GetRandomDataCommands(count int) map[string][]string {
	return GenerateRandomSQLCommands(count, MySql)
}

// Get the application type
func (app *MySqlConfig) GetApplicationType() string {
	return MySql
}

// Get Namespace of the app
func (app *MySqlConfig) GetNamespace() string {
	return app.Namespace
}
