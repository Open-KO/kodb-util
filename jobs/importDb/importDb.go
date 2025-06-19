package importDb

import (
	"context"
	"database/sql"
	"fmt"
	"kodb-util/artifacts"
	"kodb-util/config"
	"kodb-util/mssql"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	CreateManualArtifacts = false
)

type ScriptArgs struct {
	// Should use the [master] database in the connection string instead of [databaseConfig.dbname]
	isUseDefaultSystemDb bool
	// isNoTx set to true to not use a Tx fence.  See: https://go.dev/doc/database/execute-transactions#best_practices
	isNoTx bool
}

func defaultScriptArgs() ScriptArgs {
	return ScriptArgs{
		isUseDefaultSystemDb: false,
		isNoTx:               false,
	}
}

// ImportDb attempts to load all of the MSSQL .sql batch files from the OpenKO-db project into an MSSQL instance
// Some of these batches execute against the default (master) database (DB, user, login create), the rest should be
// executed using the created database named in databaseConfig.dbname
func ImportDb(ctx context.Context) (err error) {
	fmt.Println("-- Import --")

	err = importDbs(ctx)
	if err != nil {
		return err
	}

	err = importSchemas(ctx)
	if err != nil {
		return err
	}

	err = importUsers(ctx)
	if err != nil {
		return err
	}

	err = importLogins(ctx)
	if err != nil {
		return err
	}

	err = importTables(ctx)
	if err != nil {
		return err
	}

	err = importViews(ctx)
	if err != nil {
		return err
	}

	err = importStoredProcs(ctx)
	if err != nil {
		return err
	}

	return nil
}

// runScripts runs a related group of sql files.  Each file is broken down into batches (separated by the "GO" keyword)
// and then executed/commited within a transaction fence.
func runScripts(ctx context.Context, scriptArgs ScriptArgs, sqlScripts ...string) (err error) {
	if len(sqlScripts) == 0 {
		fmt.Println("WARN: No scripts to execute")
		return nil
	}

	driver := mssql.NewMssqlDbDriver()
	var tx *sql.Tx
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
			if err == nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}

		// attempt to rollback any transaction fence on error
		if tx != nil && err != nil {
			fmt.Println("Rolling back DB transaction")
			_ = tx.Rollback()
		}

		driver.CloseConnection()
	}()

	var conn *sql.DB
	if scriptArgs.isUseDefaultSystemDb {
		conn, err = driver.GetConnectionToDbName(mssql.DefaultSysDbName)
	} else {
		conn, err = driver.GetConnection()
	}
	if err != nil {
		return err
	}

	for i := range sqlScripts {
		batches := splitBatches(sqlScripts[i])

		fmt.Println(fmt.Sprintf("file contains %d batches", len(batches)))
		if len(batches) == 0 {
			// if there are no valid batches, skip this file before we open a TX
			continue
		}

		if !scriptArgs.isNoTx {
			fmt.Println("Beginning DB transaction")
			tx, err = conn.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %v", err)
			}
		}

		fmt.Print(fmt.Sprintf("Executing %d batches... ", len(batches)))
		for j := range batches {
			_, err = conn.Exec(batches[j])
			if err != nil {
				if !isIgnoreErr(err) {
					fmt.Printf("\nError executing batch [%d/%d]: %v\n", j+1, len(batches), err)
					return err
				} else {
					err = nil
				}
			}
		}
		fmt.Println(" Done")

		// transaction fence handling, when used
		if tx != nil {
			fmt.Println("Committing DB transaction")
			txErr := tx.Commit()
			if txErr != nil {
				return fmt.Errorf("failed to commit transaction: %v", txErr)
			}
			tx = nil
		}
	}

	return nil
}

func importDbs(ctx context.Context) (err error) {
	fmt.Println("-- Importing databases --")
	sArgs := defaultScriptArgs()
	sArgs.isUseDefaultSystemDb = true

	script, err := artifacts.GetCreateDatabaseScript(config.GetConfig().SchemaConfig.GameDb.Name)
	if err != nil {
		return err
	}

	if CreateManualArtifacts {
		err = artifacts.ExportDatabaseArtifact(config.GetConfig().SchemaConfig.GameDb.Name, script)
		if err != nil {
			return err
		}
	}

	return runScripts(ctx, sArgs, script)
}

func importSchemas(ctx context.Context) (err error) {
	fmt.Println("-- Importing Schemas --")
	sArgs := defaultScriptArgs()
	scripts := []string{}
	for _, schemaName := range config.GetConfig().SchemaConfig.GameDb.Schemas {
		script, err := artifacts.GetCreateSchemaScript(schemaName, config.GetConfig().SchemaConfig.GameDb.Name)
		if err != nil {
			return err
		}

		if CreateManualArtifacts {
			err = artifacts.ExportSchemaArtifact(schemaName, config.GetConfig().SchemaConfig.GameDb.Name, script)
			if err != nil {
				return err
			}
		}

		scripts = append(scripts, script)
	}

	return runScripts(ctx, sArgs, scripts...)
}

func importUsers(ctx context.Context) (err error) {
	fmt.Println("-- Importing Users --")
	sArgs := defaultScriptArgs()
	scripts := []string{}
	for _, user := range config.GetConfig().SchemaConfig.Users {
		script, err := artifacts.GetCreateUserScript(user.Name, user.Schema)
		if err != nil {
			return err
		}

		if CreateManualArtifacts {
			err = artifacts.ExportUserArtifact(user.Name, script)
			if err != nil {
				return err
			}
		}

		scripts = append(scripts, script)
	}

	return runScripts(ctx, sArgs, scripts...)
}

func importLogins(ctx context.Context) (err error) {
	fmt.Println("-- Importing Logins --")
	sArgs := defaultScriptArgs()
	sArgs.isUseDefaultSystemDb = true
	scripts := []string{}
	for _, loginName := range config.GetConfig().SchemaConfig.GameDb.Logins {
		script, err := artifacts.GetCreateLoginScript(loginName, config.GetConfig().SchemaConfig.GameDb.Name)
		if err != nil {
			return err
		}

		if CreateManualArtifacts {
			err = artifacts.ExportLoginArtifact(loginName, script)
			if err != nil {
				return err
			}
		}

		scripts = append(scripts, script)
	}

	return runScripts(ctx, sArgs, scripts...)
}

func importTables(ctx context.Context) (err error) {
	fmt.Println("-- Importing Tables --")
	scripts, err := getSqlScripts(filepath.Join(config.GetConfig().SchemaConfig.Dir, artifacts.TablesDir))
	if err != nil {
		return err
	}

	return runScripts(ctx, defaultScriptArgs(), scripts...)
}

func importViews(ctx context.Context) (err error) {
	fmt.Println("-- Importing Views --")
	scripts, err := getSqlScripts(filepath.Join(config.GetConfig().SchemaConfig.Dir, artifacts.ViewsDir))
	if err != nil {
		return err
	}

	return runScripts(ctx, defaultScriptArgs(), scripts...)
}

func importStoredProcs(ctx context.Context) (err error) {
	fmt.Println("-- Importing Stored Procedures --")
	scripts, err := getSqlScripts(filepath.Join(config.GetConfig().SchemaConfig.Dir, artifacts.StoredProcsDir))
	if err != nil {
		return err
	}

	sArgs := defaultScriptArgs()
	// It is advised to not use TX fences when a script contains BEGIN/COMMIT keywords; however, I'm not sure if that's
	// actually a problem here as those keywords are part of the body of the stored proc being created and not
	// being executed.  Either way, I don't think it hurts to skip the Tx Fence for Stored proc creation
	//
	// from the doc:
	// Use the APIs described in this section to manage transactions. Do not use transaction-related SQL statements such as
	// BEGIN and COMMIT directly—doing so can leave your database in an unpredictable state, especially in concurrent programs.
	// When using a transaction, take care not to call the non-transaction sql.DB methods directly, too, as those will execute
	// outside the transaction, giving your code an inconsistent view of the state of the database or even causing deadlocks.
	sArgs.isNoTx = true
	return runScripts(ctx, sArgs, scripts...)
}

// getSqlScripts returns the list of *.sql files from a given directory loaded into an array of strings
func getSqlScripts(dir string) (sqlScripts []string, err error) {
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory %s does not exist", dir)
	}
	fileNames, err := filepath.Glob(filepath.Join(dir, artifacts.SqlExtPattern))
	if err != nil {
		return nil, err
	}

	for i := range fileNames {
		fmt.Println(fmt.Sprintf("Reading %s", fileNames[i]))
		sqlBytes, err := os.ReadFile(fileNames[i])
		if err != nil {
			return nil, err
		}

		sqlScripts = append(sqlScripts, string(sqlBytes))
	}

	return sqlScripts, nil
}

// splitBatches breaks an MSSQL .sql dump file into batch groups.  MSSQL dump files use "GO" statements to separate
// batches.  The GO statement is not standard SQL and is only supported inside of MS SQL Management Studio, so we
// need to parse around it.
func splitBatches(sql string) (batches []string) {
	batches = strings.Split(sql, artifacts.BatchTerminator)
	for i := range batches {
		batches[i] = strings.TrimSpace(batches[i])
		if batches[i] == "" {
			batches = append(batches[:i], batches[i+1:]...)
		}
	}
	return batches
}

// isIgnoreErr checks an error to see if it can be ignored; These are errors related to
// failed DROP statements after a database clean or new setup
func isIgnoreErr(err error) bool {
	if strings.HasPrefix(err.Error(), "mssql: Cannot drop the login") ||
		strings.HasPrefix(err.Error(), "mssql: Cannot drop the user") ||
		strings.HasPrefix(err.Error(), "mssql: Cannot drop the schema") ||
		strings.HasPrefix(err.Error(), "mssql: Cannot drop the index") ||
		strings.HasPrefix(err.Error(), "mssql: Cannot drop the view") ||
		strings.HasPrefix(err.Error(), "mssql: Cannot drop the procedure") ||
		// table objects have alter statements before create
		strings.HasPrefix(err.Error(), "mssql: Cannot find the object") {
		return true
	}
	return false
}
