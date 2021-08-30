package mssql

import (
	"bytes"
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"net/url"
	"os"
	"reflect"

	_ "github.com/denisenkom/go-mssqldb"
)

type DataPullRecord struct {
	Error  error
	Values []interface{}
}

type DataPullInfo struct {
	DataSource string
	Schema     string
	DataTable  string
	DataFields []string
	Predicates []string
}

type DataPullColumn struct {
	Name        string
	Type        interface{} // pointer to a zero-value of the type
	ReflectType reflect.Type
}

func databaseConnection() (*sql.DB, error) {
	queryString := url.Values{}
	queryString.Add("app name", "Data Hook")
	queryString.Add("database", "AdventureWorks2017")
	// TODO: Need to determine if connection is a host (named instance), or a url and port. For now, only host is used.
	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword("datahook", "password"),
		Host:     fmt.Sprintf("%s:%d", "DESKTOP-CNEOG0R", 1433),
		RawQuery: queryString.Encode(),
	}

	conn, err := sql.Open("sqlserver", u.String())
	// TODO: retries
	if err != nil {
		log.Println(err)
		return nil, err
	}
	// Ping server to ensure connection is successful
	if err := conn.Ping(); err != nil {
		log.Println(err)
	} else {
		log.Println("Established connection")
	}
	return conn, err
}

func ReadFromMSSQL() {
	conn, err := databaseConnection()
	if err != nil {
		log.Println("Connection could not be established")
		os.Exit(1)
	}
	defer conn.Close()

	dp := DataPullInfo{
		DataSource: "AdventureWorks2017",
		Schema:     "Production",
		DataTable:  "TransactionHistoryArchive",
		DataFields: []string{"TransactionID", "ProductID", "ReferenceOrderID", "ReferenceOrderLineID", "TransactionDate", "TransactionType", "Quantity", "ActualCost", "ModifiedDate"},
	}

	dataStream := make(chan DataPullRecord)

	// Note: we add length here to simplify the template
	templateDetails := struct {
		DataPullInfo
		NumFields     int
		NumPredicates int
	}{
		dp,
		len(dp.DataFields) - 1,
		len(dp.Predicates) - 1,
	}

	// Build the query based on the fields to pull with predicate pushdown
	queryTemplate, err := template.New("").Parse(`
	{{$fieldLength := .NumFields}}
	{{$predicateLength := .NumPredicates}}
	USE {{.DataSource}};
	SELECT
	{{range $i, $field := .DataFields}}
		{{$field}}{{ if (lt $i $fieldLength)}},{{end}}
	{{end}}
	FROM {{.Schema}}.{{.DataTable}}
	{{range $i, $predicate := .Predicates}}
		{{$predicate}}{{if (lt $i $predicateLength )}} AND {{end}}
	{{end}}
	`)

	if err != nil {
		log.Println(err)
		close(dataStream)
		os.Exit(1)
	}

	var query bytes.Buffer
	err = queryTemplate.Execute(&query, templateDetails)
	if err != nil {
		log.Println(err)
		close(dataStream)
		os.Exit(1)
	}
	rows, err := conn.Query(query.String())
	if err != nil {
		log.Println(err)
		if rows != nil {
			if closeErr := rows.Close(); closeErr != nil {
				log.Println(err)
			}
		}
		close(dataStream)
		os.Exit(1)
	}

	columnNames, err := rows.Columns()
	if err != nil {
		log.Println(err)
		close(dataStream)
		os.Exit(1)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Println(err)
		close(dataStream)
		os.Exit(1)
	}

	dataColumns := make([]DataPullColumn, len(dp.DataFields))
	columnPointers := make([]interface{}, len(dataColumns))

	for i, v := range columnNames {
		var ptr interface{}
		var rt reflect.Type
		st := columnTypes[i].ScanType()
		switch st.String() {
		case "[]uint8":
			fl := float64(0)
			ptr = &fl
			rt = reflect.TypeOf(fl)
		default:
			// keeps the value as an interface{}, which means there will be no conversions by the SQL library
			ptr = reflect.New(st).Elem().Addr().Interface()
			rt = st
		}
		columnPointers[i] = ptr
		dataColumns[i] = DataPullColumn{v, ptr, rt}
	}

	var results [][]interface{}
	for rows.Next() {
		record := make([]interface{}, len(dataColumns))
		err = rows.Scan(columnPointers...)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		for i, v := range columnPointers {
			// TODO: Test performance implications for leaving as pointer vs. pulling the value
			record[i] = reflect.ValueOf(v).Elem().Interface()
		}
		results = append(results, record)
	}

}
