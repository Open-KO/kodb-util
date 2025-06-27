package export

import (
	"fmt"
	"kodb-util/artifacts"
	"kodb-util/config"
	"kodb-util/mssql"
	"os"
	"path/filepath"
)

const (
	// getStoredProcedureFmt extracts stored procedures from the database
	getStoredProcedureFmt = `SELECT [name], OBJECT_DEFINITION([object_id]) as [proc] FROM [sys].[procedures] WHERE [is_ms_shipped] = 0`
)

type StoredProcDef struct {
	Name string `gorm:"column:name"`
	Proc string `gorm:"column:proc"`
}

func StoredProcedures(driver *mssql.MssqlDbDriver) (err error) {
	fmt.Println("-- Exporting Stored Procedure --")
	// ensure ManualSetup directory exists
	err = os.MkdirAll(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir), os.ModePerm)
	if err != nil {
		fmt.Printf("failed to create the ManualSetup directory: %v\n", err)
		return
	}

	// clean the old export files
	files, err := filepath.Glob(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir, "[8][_]*.sql"))
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

	// pull the stored procs from the database
	storedProcs := []StoredProcDef{}
	err = gormConn.Raw(getStoredProcedureFmt).Scan(&storedProcs).Error
	if err != nil {
		return err
	}

	// write them to the output folder
	for i := range storedProcs {
		storedProcs[i].Proc = storedProcs[i].Proc + "\n"
		err = artifacts.ExportStoredProcArtifact(driver, storedProcs[i].Name, storedProcs[i].Proc)
		if err != nil {
			return err
		}
	}

	return nil
}
