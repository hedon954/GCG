package clickhouse

import (
	"fmt"

	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sirupsen/logrus"
)

type ClickHouseHook struct {
	Conn      driver.Conn
	Ctx       context.Context
	TableInfo *TableInfo
}

type TableInfo struct {
	TableName   string
	Engine      string
	Columns     map[string]string // contains level/message/time auto, please dont add these repeatly
	PrimaryKeys []string
	OrderBys    []string
}

// CreateClickHouse createa a clickhouse hook with clickhouse config
func CreateClickHouse(ctx context.Context, options *clickhouse.Options, tableInfo *TableInfo) (*ClickHouseHook, error) {

	err := checkTableInfo(tableInfo)
	if err != nil {
		return nil, err
	}

	hook := &ClickHouseHook{
		Ctx:       ctx,
		TableInfo: tableInfo,
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("cannot init clickhouse: %v", err)
	}

	hook.Conn = conn

	// init table
	err = hook.initTable()
	if err != nil {
		return nil, err
	}

	return hook, nil

}

//Fire logrus hook interface
func (hook *ClickHouseHook) Fire(entry *logrus.Entry) error {
	data := hook.newCHData(entry)
	go hook.sendToCH(data)
	return nil
}

//Levels logrus hook interface
func (hook *ClickHouseHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.DebugLevel,
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
	}
}

// newCHData creates a new data from logger entry
func (hook *ClickHouseHook) newCHData(entry *logrus.Entry) map[string]interface{} {
	ins := map[string]interface{}{}
	for k, _ := range hook.TableInfo.Columns {
		if v, ok := entry.Data[k]; ok {
			ins[k] = v
		}
	}
	ins["time"] = entry.Time.Format("2006-01-02 15:04:05")
	ins["level"] = entry.Level
	ins["message"] = entry.Message
	return ins
}

// sendToCH sends data to clickhouse
func (hook *ClickHouseHook) sendToCH(data map[string]interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("send data to clickhouse failed: %v", r)
		}
	}()

	rows := ""
	values := make([]interface{}, 0)
	valueStr := ""

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	for i := 0; i < len(keys); i++ {
		rows += keys[i]
		valueStr += "'%v'"
		if i != len(keys)-1 {
			rows += ", "
			valueStr += ", "
		}
		values = append(values, data[keys[i]])
	}

	valueStr = fmt.Sprintf(valueStr, values...)

	insertSql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", hook.TableInfo.TableName, rows, valueStr)

	err := hook.Conn.Exec(hook.Ctx, insertSql, false)
	if err != nil {
		fmt.Printf("insert data to clickhouse failed: %v", err)
	}
}

// ClearTable clears clickhouse table
func (hook *ClickHouseHook) ClearTable() error {
	if err := hook.Conn.Exec(hook.Ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, hook.TableInfo.TableName)); err != nil {
		return err
	}
	return nil
}

// initTable initialzes clickhouse table according to tableInfo
func (hook *ClickHouseHook) initTable() error {

	// columns
	columns := ""
	for k, v := range hook.TableInfo.Columns {
		columns += fmt.Sprintf("%s %s,", k, v)
	}

	columns += "level String, message String, time Datetime"

	// primary key
	primarys := "("
	for i := 0; i < len(hook.TableInfo.PrimaryKeys); i++ {
		primarys += hook.TableInfo.PrimaryKeys[i]
		if i != len(hook.TableInfo.PrimaryKeys)-1 {
			primarys += ", "
		}
	}
	primarys += ")"

	// orders
	orders := "("
	for i := 0; i < len(hook.TableInfo.OrderBys); i++ {
		orders += hook.TableInfo.OrderBys[i]
		if i != len(hook.TableInfo.OrderBys)-1 {
			orders += ", "
		}
	}
	orders += ")"

	if err := hook.Conn.Exec(hook.Ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			%s
		 ) ENGINE = %s
		 PARTITION BY toYYYYMMDD(time) 
		 PRIMARY KEY %s
		 ORDER BY %s;
		`,
		hook.TableInfo.TableName,
		columns,
		hook.TableInfo.Engine,
		primarys,
		orders,
	)); err != nil {
		return err
	}
	return nil
}

func checkTableInfo(tableInfo *TableInfo) error {
	if tableInfo == nil {
		return fmt.Errorf("tableInfo is nil")
	}
	if tableInfo.TableName == "" {
		return fmt.Errorf("table name should not be empty")
	}
	if tableInfo.Engine == "" {
		return fmt.Errorf("table engine should not be empty")
	}
	if len(tableInfo.Columns) == 0 {
		return fmt.Errorf("table columns are not defined")
	}
	if len(tableInfo.PrimaryKeys) == 0 {
		return fmt.Errorf("table primary keys are not defined")
	}
	if len(tableInfo.OrderBys) == 0 {
		return fmt.Errorf("table order by keys are not defined")
	}

	keys := make([]string, 0, len(tableInfo.Columns))
	for k := range tableInfo.Columns {
		keys = append(keys, k)
	}

	if !sliceContains(keys, tableInfo.PrimaryKeys) {
		return fmt.Errorf("primary keys are not contained in columns")
	}

	if !sliceContains(keys, tableInfo.OrderBys) {
		return fmt.Errorf("order keys are not contained in columns")
	}

	return nil
}

func sliceContains(s1 []string, s2 []string) bool {
	for i := 0; i < len(s2); i++ {
		has := false
		for j := 0; j < len(s1); j++ {
			if s2[i] == s1[j] {
				has = true
				break
			}
		}
		if !has {
			return false
		}
	}

	return true
}
