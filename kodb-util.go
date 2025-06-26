package main

import (
	"context"
	"fmt"
	"github.com/kenner2/OpenKO-db/jsonSchema/enums/dbType"
	"github.com/kenner2/openko-gorm/kogen"
	"gorm.io/gorm"
	"kodb-util/config"
	"kodb-util/jobs/clean"
	"kodb-util/jobs/export"
	"kodb-util/jobs/importDb"
	"kodb-util/mssql"
	"log"
	"strings"
)

const (
	appTitle    = "OpenKO Database Utilities"
	outputWidth = 120
)

type dbInfo struct {
	Type   dbType.DbType
	Config config.GenDbConfig
}

func printHeaderRow() {
	fmt.Println(fmt.Sprintf("%s", strings.Repeat("-", outputWidth)))
}

func main() {
	defer func() {
		// catch-all panic error
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
		}
	}()

	// Print intro header
	printHeaderRow()
	titlePad := (outputWidth - len(appTitle)) / 2
	fmt.Println(fmt.Sprintf("%[2]s%[1]s%[2]s", appTitle, strings.Repeat(" ", titlePad)))
	printHeaderRow()

	args := getArgs()
	if err := args.Validate(); err != nil {
		fmt.Printf("arguments error: %v, closing.", err)
		return
	}

	// loading config for the first time can throw a panic, so let's do it here
	// uses a singleton pattern, so once loaded from disk it's in memory
	fmt.Print("Loading config...")
	conf := config.GetConfig()
	// apply any command-line overrides
	if args.DbUser != "" {
		conf.DatabaseConfig.User = args.DbUser
	}
	if args.DbPass != "" {
		conf.DatabaseConfig.Password = args.DbPass
	}
	if args.SchemaDir != "" {
		conf.GenConfig.SchemaDir = args.SchemaDir
	}
	if args.ImportBatchSize > 1 && args.ImportBatchSize < 1000 {
		importDb.ImportBatSize = args.ImportBatchSize
	}
	fmt.Println("done")

	// Create a stub context for use with our db-ops.  We're not doing anything fancy with it now, but it will give us a
	// few options if we ever desire them (deadlines, cancel funcs, key:val mapping)
	// https://pkg.go.dev/context
	appCtx := context.Background()

	dbs := []dbInfo{}
	for i := range conf.GenConfig.GameDbs {
		dbs = append(dbs, dbInfo{
			Config: conf.GenConfig.GameDbs[i],
			Type:   dbType.GAME,
		})
	}

	// TODO: Add multi-db support by updating the config structure with LoginDbs and LogDbs
	// and adding them to the dbs list

	for i := range dbs {
		err := processDb(appCtx, dbs[i], args)
		if err != nil {
			panic(err)
		}
	}
}

// processDb attempts requested jobs for the given database
func processDb(appCtx context.Context, db dbInfo, args Args) (err error) {
	// a clean driver should be used/configured per database as the application logic
	// makes heavy use of the driver.GenDbConfig
	driver := mssql.NewMssqlDbDriver(db.Config, db.Type)

	var tx *gorm.DB
	defer func() {
		// catch-all panic error
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
		if tx != nil {
			if err != nil {
				rErr := driver.RollbackTx()
				if rErr != nil {
					fmt.Printf("failed to rollback transaction: %v", rErr)
				}
			} else {
				err = driver.CommitTx()
			}
		}
		driver.CloseConnection()
	}()

	// Set the model package DB Name
	// kogen.SetDbNameByType(kogen.DbType(driver.DbType), driver.GenDbConfig.Name)
	kogen.SetLoginDbName(driver.GenDbConfig.Name)
	kogen.SetGameDbName(driver.GenDbConfig.Name)
	kogen.SetLogDbName(driver.GenDbConfig.Name)

	// Run clean if either -clean or -import was called
	if args.Clean || args.Import {
		if driver.GenDbConfig.IsForbidClean {
			fmt.Printf("WARN: clean operation for %s database is forbidden, skipping -clean action\n", driver.GenDbConfig.Name)
		} else {
			err = clean.Clean(appCtx, driver)
			if err != nil {
				return err
			}
		}
	}

	if args.Import {
		if driver.GenDbConfig.IsForbidImport || driver.GenDbConfig.IsForbidClean {
			fmt.Printf("WARN: clean or Import operation for %s database is forbidden, skipping -import action\n", driver.GenDbConfig.Name)
		} else {
			err = importDb.ImportDb(appCtx, driver)
			if err != nil {
				return err
			}
		}
	}

	if driver.GenDbConfig.IsForbidExport && (args.ExportStructure || args.ExportData || args.ExportJsonSchema || args.ExportAll) {
		fmt.Printf("WARN: export operation for %s database is forbidden, skipping -export* actions\n", driver.GenDbConfig.Name)
		return nil
	}

	// ImportDb will set driver.Tx as it has a mix of work to do on master/gen databases.  Get a ref to that pointer,
	// or open it now if import wasn't called
	tx, err = driver.GetTx()
	if err != nil {
		return err
	}

	if args.ExportJsonSchema {
		err = export.JsonSchema(driver)
		if err != nil {
			return err
		}
	}

	if args.ExportStructure || args.ExportAll {
		err = export.Structure(driver)
		if err != nil {
			return err
		}
	}

	if args.ExportData || args.ExportAll {
		err = export.TableData(driver)
		if err != nil {
			return err
		}
	}

	return nil
}
