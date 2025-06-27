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
	// getViewsSqlFmt extracts views from the database
	getViewsSqlFmt = `SELECT [name], OBJECT_DEFINITION([object_id]) as [aView] FROM [sys].[views] WHERE [is_ms_shipped] = 0;`
)

type ViewDef struct {
	Name string `gorm:"column:name"`
	View string `gorm:"column:aView"`
}

func Views(driver *mssql.MssqlDbDriver) (err error) {
	fmt.Println("-- Exporting Views --")
	// ensure ManualSetup directory exists
	err = os.MkdirAll(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir), os.ModePerm)
	if err != nil {
		fmt.Printf("failed to create the ManualSetup directory: %v\n", err)
		return
	}

	// clean the old export files
	files, err := filepath.Glob(filepath.Join(config.GetConfig().GenConfig.SchemaDir, artifacts.ManualSetupDir, "[7][_]*.sql"))
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

	// pull the views from the database
	views := []ViewDef{}
	err = gormConn.Raw(getViewsSqlFmt).Scan(&views).Error
	if err != nil {
		return err
	}

	// write them to the output folder
	for i := range views {
		views[i].View = views[i].View + "\n"
		err = artifacts.ExportViewArtifact(driver, views[i].Name, views[i].View)
		if err != nil {
			return err
		}
	}

	return nil
}
