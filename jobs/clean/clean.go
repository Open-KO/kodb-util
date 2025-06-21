package clean

import (
	"context"
	"database/sql"
	"fmt"
	"kodb-util/config"
	"kodb-util/mssql"
	"log"
	"strings"
)

const (
	dropUserSqlFmt = "DROP LOGIN [%s]"
	dropDbSqlFmt   = "DROP DATABASE IF EXISTS [%s]"
)

// Clean will remove any existing [databaseConfig.dbname] database and knight user from an mssql instance
func Clean(ctx context.Context) (err error) {
	fmt.Println("-- Clean --")
	driver := mssql.NewMssqlDbDriver()
	var tx *sql.Tx
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
			if err == nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}

		if tx != nil && err != nil {
			fmt.Println("Rolling back transaction")
			_ = tx.Rollback()
		}

		driver.CloseConnection()
	}()

	// Get a connection to the [master] database
	conn, err := driver.GetConnectionToDbName(mssql.DefaultSysDbName)
	if err != nil {
		return err
	}

	// start session
	fmt.Println("Starting transaction")
	tx, err = conn.BeginTx(ctx, nil)

	fmt.Print(fmt.Sprintf("Dropping %s database... ", config.GetConfig().SchemaConfig.GameDb.Name))
	_, err = conn.Exec(fmt.Sprintf(dropDbSqlFmt, config.GetConfig().SchemaConfig.GameDb.Name))
	if err != nil {
		return err
	}
	fmt.Println(" Done")

	// Drop the users we're about to create
	for _, user := range config.GetConfig().SchemaConfig.Users {
		fmt.Print(fmt.Sprintf("Dropping user %s... ", user.Name))
		_, err = conn.Exec(fmt.Sprintf(dropUserSqlFmt, user.Name))
		if err != nil {
			// ignore failed drop error - user may not exist.
			if !strings.HasPrefix(err.Error(), "mssql: Cannot drop the login") {
				return err
			}
			fmt.Print(" Not found.")
			err = nil
		}
		fmt.Println(" Done")
	}

	fmt.Println("Committing transaction")
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return err
}
