package parquet

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"
	"unicode"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type DataPullColumn struct {
	Name        string
	Type        interface{} // pointer to a zero-value of the type
	ReflectType reflect.Type
}

func WriteToCSV(results [][]interface{}) {
	// Save CSV
	f, err := os.Create("output/flat.csv")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	w := csv.NewWriter(f)

	for _, v := range results {
		w.Write(func() []string {
			s := make([]string, len(v))
			for i, val := range v {
				s[i] = fmt.Sprintf("%v", val)
			}
			return s
		}())
	}
}

func WriteToParquet(dataColumns []DataPullColumn, results [][]interface{}) {
	// capitalizeFirst capitalizes the first letter of a string. This is safe to use with unicode characters
	capitalizeFirst := func(s string) string {
		a := []rune(s)
		a[0] = unicode.ToUpper(a[0])
		return string(a)
	}

	// Save as Parquet file
	schema := make([]string, len(dataColumns))

	for i, v := range dataColumns {
		switch v.Type.(type) {
		case *string:
			schema[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY", capitalizeFirst(v.Name)) // plain_dictionary is used when columns have many of the same values
		case *int, *int8, *int16, *int32:
			schema[i] = fmt.Sprintf("name=%s, type=INT32", capitalizeFirst(v.Name))
		case *int64:
			schema[i] = fmt.Sprintf("name=%s, type=INT64", capitalizeFirst(v.Name))
		case *float32:
			schema[i] = fmt.Sprintf("name=%s, type=FLOAT", capitalizeFirst(v.Name))
		case *float64:
			schema[i] = fmt.Sprintf("name=%s, type=DOUBLE", capitalizeFirst(v.Name))
		case *bool:
			schema[i] = fmt.Sprintf("name=%s, type=BOOLEAN", capitalizeFirst(v.Name))
		case *time.Time:
			schema[i] = fmt.Sprintf("name=%s, type=INT64, convertedtype=TIMESTAMP_MILLIS", capitalizeFirst(v.Name))
		default:
			// Unknown type, encode it as a byte array
			schema[i] = fmt.Sprintf("name=%s, type=INT32, convertedtype=UINT_8", capitalizeFirst(v.Name)) // If we don't know what it is, store it as []uint8
		}
	}

	fw, err := local.NewLocalFileWriter("output/flat.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}
	defer fw.Close()

	// Note: Parquet can only receive concrete types. These need to be convereted before writing
	// Write as CSV
	pw, err := writer.NewCSVWriter(schema, fw, numCPUs) // number of goroutines is the number of processors
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	// Ensures all types are converted to the proper parquet primitive type
	// TODO: This will need to be a separate "toParquet" function or package
	for _, v := range results {
		// Convert time.Time to millis
		// Note: parquet library includes more robust conversions than this if required
		for j := range v {
			switch t := v[j].(type) {
			case time.Time:
				v[j] = t.Unix() * 1000.0
			}
		}
		if err = pw.Write(v); err != nil {
			log.Println("Write error", err)
			break
		}
	}

	// Must call WriteStop() to flush the data (these used to be separate calls at one point)
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}

}

// General consensus is:

// Writing will automatically handle buffering for you based on the page sizes
// Reading you will need to limit the rows yourself that are passing through memory (e.g., read it like a stream)
