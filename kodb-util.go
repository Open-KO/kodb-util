package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/kenner2/openko-gorm/kogen"
	"kodb-util/artifacts"
	"kodb-util/config"
	"kodb-util/jobs/clean"
	"kodb-util/jobs/exportJsonSchema"
	"kodb-util/jobs/importDb"
	"log"
	"os"
	"path/filepath"
)

const (
	defaultOpenKoDbDir = "./OpenKO-db"
)

type Args struct {
	Clean                 bool
	Import                bool
	Export                bool
	ConfigPath            string
	DbUser                string
	DbPass                string
	SchemaDir             string
	CreateManualArtifacts bool
	ExportJsonSchema      bool
}

// Validate ensures that the combination of arguments used is valid
func (this Args) Validate() (err error) {
	if !(this.Clean || this.Import || this.Export || this.ExportJsonSchema) {
		flag.Usage()
		return fmt.Errorf("no actionable arguments provided")
	}
	if this.Clean && (this.Export || this.ExportJsonSchema) {
		return fmt.Errorf("cannot perform both clean and export actions")
	}
	if this.Import && this.Export {
		// maybe to test that nothing changes, but for general use would be an expensive no-op
		// could do a separate arg/job for diff checking
		return fmt.Errorf("running import and export together is redundant")
	}

	return nil
}

func getArgs() (a Args) {
	_clean := flag.Bool("clean", false, "Clean drops the databaseConfig.dbname database and removes the knight user")
	_import := flag.Bool("import", false, "Runs clean and imports OpenKO-db files")
	exportDb := flag.Bool("export", false, "Export the database to OpenKO-db files")
	createManualArtifacts := flag.Bool("createManualArtifacts", false, "Export the artifacts generated from templates during the import process to OpenKO-db/ManualSetup.  Performs a clean on the ManualSetup directory first.")
	exportJsonSchema := flag.Bool("exportJsonSchema", false, "Export table properties from the database to update jsonSchema")
	configPath := flag.String("config", config.DefaultConfigFileName, "Path to config file")
	dbUser := flag.String("dbuser", "", "Database user override")
	dbPass := flag.String("dbpass", "", "Database password override")
	schemaDir := flag.String("schema", "", "OpenKO-db schema directory override")

	flag.Parse()

	if _clean != nil {
		a.Clean = *_clean
	}

	if _import != nil {
		a.Import = *_import
	}

	if createManualArtifacts != nil {
		a.CreateManualArtifacts = *createManualArtifacts
	}

	if exportJsonSchema != nil {
		a.ExportJsonSchema = *exportJsonSchema
	}

	if exportDb != nil {
		a.Export = *exportDb
	}

	if configPath != nil {
		a.ConfigPath = *configPath
		config.ConfigPath = *configPath
	}

	if dbUser != nil {
		a.DbUser = *dbUser
	}

	if dbPass != nil {
		a.DbPass = *dbPass
	}

	if schemaDir != nil {
		a.SchemaDir = *schemaDir
	}

	return a
}

func main() {
	defer func() {
		// catch-all panic error capture
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
		}
	}()

	fmt.Println("|---------------------------|")
	fmt.Println("| OpenKO Database Utilities |")
	fmt.Println("|---------------------------|")

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
		conf.SchemaConfig.Dir = args.SchemaDir
	}

	// Set GORM database names from config
	// TODO:  When we make the change to multi-db, these params will need to be updated to support it
	gameDbName := conf.SchemaConfig.GameDb.Name
	kogen.SetDbNames(gameDbName, gameDbName, gameDbName)
	fmt.Println(" done")

	if conf.DatabaseConfig.User == "" {
		fmt.Println("No database user specified. Windows Authentication will be attempted")
	}

	// Create a stub context for use with our db-ops.  We're not doing anything fancy with it now, but it will give us a
	// few options if we ever desire them (deadlines, cancel funcs, key:val mapping)
	// https://pkg.go.dev/context
	appCtx := context.Background()

	// Run clean if either -clean or -import was called
	if args.Clean || args.Import {
		err := clean.Clean(appCtx)
		if err != nil {
			panic(err)
		}
	}

	if args.Import {
		if args.CreateManualArtifacts {
			importDb.CreateManualArtifacts = true
			fmt.Println("Import process will write template-generated artifacts to OpenKO-db/ManualSetup")
			err := os.RemoveAll(filepath.Join(conf.SchemaConfig.Dir, artifacts.ManualSetupDir))
			if err != nil {
				fmt.Printf("failed to clean the ManualSetup directory: %w\n", err)
				return
			}
			// ensure ManualSetup directory exists
			err = os.MkdirAll(filepath.Join(conf.SchemaConfig.Dir, artifacts.ManualSetupDir), os.ModePerm)
			if err != nil {
				fmt.Printf("failed to create the ManualSetup directory: %w\n", err)
				return
			}
		}

		err := importDb.ImportDb(appCtx)
		if err != nil {
			panic(err)
		}
	}

	if args.ExportJsonSchema {
		err := exportJsonSchema.ExportJsonSchema()
		if err != nil {
			panic(err)
		}
	}

}
