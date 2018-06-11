package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"
)

var mysqlConnectionBackoff = wait.Backoff{
	Duration: 2 * time.Second,
	Factor:   1,
	Jitter:   0.1,
	Steps:    10,
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ",")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// command line arguments
var (
	hostList arrayFlags
	hostPort int64
)

const (
	driverNameMysql  = "mysql"
	mysqlDefaultUser = "root"
	userEnvKey       = "MYSQL_USER"
	passwordEnvKey   = "MYSQL_PASSWORD"
)

func main() {
	logrus.Infof("Running mysql locker: %v", version.Version)
	flag.Parse()

	if len(hostList) == 0 {
		logrus.Fatalf("no mysql hosts specified to the locker")
	}

	// Get username and password from env
	user := os.Getenv(userEnvKey)
	if len(user) == 0 {
		logrus.Infof("using default user: %s", mysqlDefaultUser)
		user = mysqlDefaultUser
	}

	password := os.Getenv(passwordEnvKey)
	if len(password) == 0 {
		logrus.Fatalf("no password provided for mysql connection. Export the mysql password using env variable: %s", passwordEnvKey)
	}

	err := flushAndLock(hostList, hostPort, user, password)
	if err != nil {
		logrus.Fatalf("failed to flush and lock mysql hosts: %s due to err: %v", hostList, err)
	}
}

func flushAndLock(hosts []string, port int64, user, password string) error {
	connections := make([]*sql.DB, 0)
	for _, host := range hosts {
		err := wait.ExponentialBackoff(mysqlConnectionBackoff, func() (bool, error) {
			db, err := sql.Open(driverNameMysql, fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port))
			if err != nil {
				logrus.Errorf("failed to open mysql connection to: %s due to: %v", host, err)
				return false, nil
			}

			connections = append(connections, db)

			logrus.Infof("flushing database and acquiring read lock on: %s", host)
			l, err := db.Query("flush tables with read lock")
			if err != nil {
				logrus.Errorf("query failed due to: %v", err)
				return false, nil
			}

			defer l.Close()
			return true, nil
		})
		if err != nil {
			return err
		}
	}

	defer func() {
		logrus.Infof("closing connections...")
		for _, conn := range connections {
			conn.Close()
		}
	}()

	for {
	}
}

func init() {
	flag.Var(&hostList, "host", "Mysql host. For more than one hosts, supply multiple -host arguments.")
	flag.Int64Var(&hostPort, "port", 3306, "(Optional) Mysql port.")
}
