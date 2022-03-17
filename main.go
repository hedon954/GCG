package main

import (
	"context"
	"fmt"
	"log"
	"time"

	chPkg "GCG/hook/clickhouse"
	"GCG/logger"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
)

func main() {

	ctx := context.Background()

	clickHouseHook, err := chPkg.CreateClickHouse(ctx, &clickhouse.Options{
		Addr: []string{"172.16.208.160:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:  5 * time.Second,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	}, &chPkg.TableInfo{
		TableName: "GCG_log",
		Engine:    "MergeTree",
		Columns: map[string]string{
			"id":     "UInt32",
			"log_id": "String",
		},
		PrimaryKeys: []string{"id"},
		OrderBys:    []string{"id", "log_id"},
	})

	if err != nil {
		log.Fatal(err)
	}

	logUtil := logger.Default(clickHouseHook)

	for i := 0; i < 100; i++ {
		logUtil.WithFields(logrus.Fields{
			"id":     (uint32)(i),
			"log_id": fmt.Sprintf("log_%d", i),
		}).Infof("hedon-index-%d", i)
	}

	time.Sleep(100 * time.Second)

}
