package export

import (
	"fmt"
	"github.com/Open-KO/OpenKO-gorm/kogen"
	"kodb-util/artifacts"
	"kodb-util/config"
	"kodb-util/mssql"
	"os"
	"path/filepath"
	"strings"
)

// TableData uses the openko-gorm model library to query all table data in a way that preserves original values
// and uses those model objects to generate insert dumps as OpenKO-db/ManualSetup/6_InsertData_*.sql
func TableData(driver *mssql.MssqlDbDriver) (err error) {
	fmt.Println("-- Exporting Table Data --")
	// ensure ManualSetup directory exists
	err = os.MkdirAll(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir), os.ModePerm)
	if err != nil {
		fmt.Printf("failed to create the ManualSetup directory: %v\n", err)
		return
	}

	// clean the old export files
	files, err := filepath.Glob(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir, "[6][_]*.sql"))
	if err != nil {
		return err
	}
	for i := range files {
		if err = os.Remove(files[i]); err != nil {
			return err
		}
	}

	gormConn, err := driver.GetConnection()
	if err != nil {
		return err
	}

	// iterate over the tables in our schema and extract their data
	for i := range kogen.ModelList {
		var results []kogen.Model
		results, err = kogen.ModelList[i].GetAllTableData(gormConn)
		if err != nil {
			return err
		}

		// only write an insert dump if the table had data
		if len(results) > 0 {
			sb := strings.Builder{}
			sb.WriteString(kogen.ModelList[i].GetInsertHeader())
			for j := range results {
				if j > 0 {
					sb.WriteString(",\n")
				}
				sb.WriteString(results[j].GetInsertData())
			}
			// ensure EOF empty line
			sb.WriteString("\n")
			err = artifacts.ExportTableDataArtifact(driver, kogen.ModelList[i].TableName(), sb.String())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
