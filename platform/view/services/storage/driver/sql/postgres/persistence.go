/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package postgres

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/hyperledger-labs/fabric-smart-client/platform/common/services/logging"
	"github.com/hyperledger-labs/fabric-smart-client/platform/common/utils/lazy"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/storage/driver/common"
	common2 "github.com/hyperledger-labs/fabric-smart-client/platform/view/services/storage/driver/sql/common"
)

var logger = logging.MustGetLogger()

const driverName = "pgx"

type DbProvider interface {
	Get(Opts) (*common.RWDB, error)
}

func NewDbProvider() DbProvider { return lazy.NewProviderWithKeyMapper(key, Open) }

func key(o Opts) string { return o.DataSource + "|" + o.WriteDataSource }

func Open(opts Opts) (*common.RWDB, error) {
	readDB, err := openDB(opts.DataSource, opts.MaxOpenConns, opts.MaxIdleConns, opts.MaxIdleTime, opts.Tracing)
	if err != nil {
		return nil, fmt.Errorf("can't open %s database: %w", driverName, err)
	}

	if len(opts.WriteDataSource) == 0 {
		logger.Debugf("connected to [%s] shared read/write pool, max open connections: %d, max idle connections: %d, max idle time: %v", driverName, opts.MaxOpenConns, opts.MaxIdleConns, opts.MaxIdleTime)

		return &common.RWDB{
			ReadDB:  readDB,
			WriteDB: readDB,
		}, nil
	}

	writeDB, err := openDB(opts.WriteDataSource, opts.WriteMaxOpenConns, opts.WriteMaxIdleConns, opts.WriteMaxIdleTime, opts.Tracing)
	if err != nil {
		readDB.Close()
		return nil, fmt.Errorf("can't open %s write database: %w", driverName, err)
	}
	logger.Debugf("connected to [%s] split pools, read max open connections: %d, write max open connections: %d", driverName, opts.MaxOpenConns, opts.WriteMaxOpenConns)

	return &common.RWDB{
		ReadDB:  readDB,
		WriteDB: writeDB,
	}, nil
}

func openDB(dataSource string, maxOpenConns, maxIdleConns int, maxIdleTime time.Duration, tracing *common2.TracingConfig) (*sql.DB, error) {
	db, err := sqlOpen(dataSource, tracing)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxIdleTime(maxIdleTime)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func sqlOpen(dataSourceName string, tracing *common2.TracingConfig) (*sql.DB, error) {
	if tracing == nil {
		return sql.Open(driverName, dataSourceName)
	}
	return otelsql.Open(driverName, dataSourceName,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
	)
}

type Opts struct {
	DataSource        string
	MaxOpenConns      int
	MaxIdleConns      int
	MaxIdleTime       time.Duration
	WriteDataSource   string
	WriteMaxOpenConns int
	WriteMaxIdleConns int
	WriteMaxIdleTime  time.Duration
	TablePrefix       string
	TableNameParams   []string
	Tracing           *common2.TracingConfig
}
