package main

import (
	"context"
	"flag"
	"fmt"
	"kodb-util/config"
	"kodb-util/jobs/clean"
	"kodb-util/jobs/importDb"
	"log"
)

const (
	defaultOpenKoDbDir = "../OpenKO-db"
)

type Args struct {
	Clean  bool
	Import bool
	// TODO
	//Export bool
	ConfigPath string
	DbUser     string
	DbPass     string
	SchemaDir  string
}

func (this Args) HasActionableArgs() bool {
	if this.Clean || this.Import {
		return true
	}
	return false
}

func getArgs() (a Args) {
	_clean := flag.Bool("clean", false, "Clean drops the databaseConfig.dbname database and removes the knight user")
	_import := flag.Bool("import", false, "Runs clean and imports OpenKO-db files")
	//exportDb := flag.Bool("export", false, "Export the KN_online database to OpenKO-db files")
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

	// TODO
	//if exportDb != nil {
	//	a.Export = *exportDb
	//}

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
	if !args.HasActionableArgs() {
		fmt.Println("No arguments provided:")
		flag.Usage()
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
		err := importDb.ImportDb(appCtx)
		if err != nil {
			panic(err)
		}
	}

}
