package arg

import (
	"flag"
	"fmt"
	"kodb-util/config"
)

// Args defines and handles the CLI input flags/arguments
type Args struct {
	Clean                 bool
	Import                bool
	ImportBatchSize       int
	ExportAll             bool
	ExportData            bool
	ExportStructure       bool
	ExportProcs           bool
	ExportViews           bool
	ExportJsonSchema      bool
	ConfigPath            string
	DbUser                string
	DbPass                string
	SchemaDir             string
	CreateManualArtifacts bool
}

// Validate ensures that the combination of arguments used is valid
func (this Args) Validate() (err error) {
	if !(this.Clean || this.Import || this.HasExportJob()) {
		flag.Usage()
		return fmt.Errorf("no actionable arguments provided")
	}
	if this.Clean && this.HasExportJob() {
		return fmt.Errorf("cannot perform both clean and export actions")
	}
	if this.Import && this.HasExportJob() {
		// maybe to test that nothing changes, but for general use would be an expensive no-op
		// could do a separate arg/job for diff checking
		return fmt.Errorf("running import and export together is redundant")
	}

	return nil
}

func (this Args) HasExportJob() bool {
	if this.ExportAll || this.ExportJsonSchema || this.ExportData || this.ExportStructure || this.ExportProcs || this.ExportViews {
		return true
	}
	return false
}

// GetArgs reads the CLI arguments using the go flag package
func GetArgs() (a Args) {
	_clean := flag.Bool("clean", false, "Clean drops any configured users and drops the databaseConfig.dbname database")
	_import := flag.Bool("import", false, "Runs clean and imports the contents of OpenKO-db/ManaualSetup, StoredProcedures, and Views")
	importBatchSize := flag.Int("batchSize", 16, "Batch sized used when importing table data.  Valid range [2-999], if invalid value specified will default to 16")
	exportAll := flag.Bool("exportAll", false, "Export both the data and structure of the database")
	exportData := flag.Bool("exportData", false, "Export table data from the database")
	exportStructure := flag.Bool("exportStructure", false, "Export the structural elements of the database")
	exportProcs := flag.Bool("exportProcs", false, "Export the stored procedures of the database")
	exportViews := flag.Bool("exportViews", false, "Export the views of the database")
	exportJsonSchema := flag.Bool("exportJsonSchema", false, "Export table properties from the database to update jsonSchema.  Not part of -exportAll")
	configPath := flag.String("config", config.DefaultConfigFileName, "Path to config file, inclusive of the filename")
	dbUser := flag.String("dbuser", "", "Database connection user override")
	dbPass := flag.String("dbpass", "", "Database connection password override")
	schemaDir := flag.String("schema", "", "OpenKO-db schema directory override; in most cases you'll just want to use the default git submodule location")

	flag.Parse()

	if _clean != nil {
		a.Clean = *_clean
	}

	if _import != nil {
		a.Import = *_import
	}

	if exportJsonSchema != nil {
		a.ExportJsonSchema = *exportJsonSchema
	}

	if exportAll != nil {
		a.ExportAll = *exportAll
	}

	if exportData != nil {
		a.ExportData = *exportData
	}

	if exportStructure != nil {
		a.ExportStructure = *exportStructure
	}

	if exportProcs != nil {
		a.ExportProcs = *exportProcs
	}

	if exportViews != nil {
		a.ExportViews = *exportViews
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

	if importBatchSize != nil {
		a.ImportBatchSize = *importBatchSize
	}

	return a
}
