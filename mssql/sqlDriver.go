package mssql

import (
	"database/sql"
	"fmt"
	_ "github.com/microsoft/go-mssqldb"
	"kodb-util/config"
	"net/url"
)

// mssql sql driver impl, see: https://github.com/denisenkom/go-mssqldb

const (
	driverName = "sqlserver"

	// 1: Username
	// 2: Password
	// 3: Host
	// 4: Port
	// 5: Instance Name
	// 6: Database Name
	connStringFmt = "sqlserver://%[1]s:%[2]s@%[3]s:%[4]d/%[5]s?database=%[6]s"

	// 1: Host
	// 2: Port
	// 2: Instance
	// 3: Database
	winAuthConnStrFmt = "sqlserver://@%[1]s:%[2]d/%[3]s?database=%[4]s"

	DefaultSysDbName = "master"
)

type MssqlDbDriver struct {
	dbConfig   config.DatabaseConfig
	connString string
	conn       *sql.DB
}

func NewMssqlDbDriver() *MssqlDbDriver {
	return &MssqlDbDriver{
		dbConfig: config.GetConfig().DatabaseConfig,
	}
}

func (this *MssqlDbDriver) GetConnectionString(dbName string) string {
	if this.dbConfig.User == "" {
		// Attempt Windows Auth
		this.connString = fmt.Sprintf(winAuthConnStrFmt, this.dbConfig.Host, this.dbConfig.Port, this.dbConfig.Instance, dbName)
	} else {
		// Used Mixed Auth
		this.connString = fmt.Sprintf(connStringFmt, this.dbConfig.User, url.QueryEscape(this.dbConfig.Password), this.dbConfig.Host, this.dbConfig.Port, this.dbConfig.Instance, dbName)
	}

	return this.connString
}

func (this *MssqlDbDriver) GetConnection() (*sql.DB, error) {
	return this.GetConnectionToDbName(this.dbConfig.DbName)
}

func (this *MssqlDbDriver) GetConnectionToDbName(dbName string) (*sql.DB, error) {
	// if there's a connection already open, use it
	if this.conn != nil {
		return this.conn, nil
	}

	// otherwise, open a new connection
	fmt.Println("Opening connection to mssql")
	var err error
	this.conn, err = sql.Open(driverName, this.GetConnectionString(dbName))
	if err != nil {
		return nil, fmt.Errorf("Failed to open connection to mssql: %v", err)
	}

	return this.conn, nil
}

func (this *MssqlDbDriver) CloseConnection() {
	fmt.Println("Closing connection to mssql")
	if this.conn != nil {
		_ = this.conn.Close()
	}
	this.conn = nil
}
