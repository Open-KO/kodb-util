package utils

import (
	"context"
	"gorm.io/gorm/logger"
	"time"
)

// GormCaptureLogger can be used to collect SQL traces from GORM
type GormCaptureLogger struct {
	logger.Interface
	// effectively a hashset - due to the way their logger implementation is working, the same
	// logger.Trace function gets called multiple times by transaction fence layering
	// Using a set reduces the captures to unique queries.
	SqlLines map[string]int

	// we use captureCounter so that we can sort the SQL lines, as a hashmap doesn't preserve order
	captureCount int
}

func (l GormCaptureLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	//fmt.Println("Trace called!")
	sql, _ := fc()
	if l.SqlLines != nil {
		l.SqlLines[sql] = l.captureCount
		l.captureCount++
	}

	// should we actually call super()?  Have to play in the debugger
	l.Interface.Trace(ctx, begin, fc, err)
}
