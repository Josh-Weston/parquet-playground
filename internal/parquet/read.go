package parquet

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/josh-weston/go/util"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

// Generic information about a partition
type Partition interface {
	CloseChannel()
	ReadAllValues() chan interface{} // a convenience function for passing through all values from the channel
	ReadValue() (interface{}, bool)  // a convenience function for reading the value as an interface. Must ensure no other operation is reading from the channel
	SendValue(interface{}) error     // a way to send a value on the channel
}

// The specific type of partition
type PartitionFloat64 struct {
	Ch chan float64
}

func (p *PartitionFloat64) CloseChannel() {
	close(p.Ch)
}

func (p *PartitionFloat64) ReadAllValues() chan interface{} {
	c := make(chan interface{})
	go func() {
		defer close(c)
		for v := range p.Ch {
			c <- v
		}
	}()
	return c
}

func (p *PartitionFloat64) ReadValue() (interface{}, bool) {
	var i interface{}
	i, ok := <-p.Ch
	return i, ok
}

func (p *PartitionFloat64) SendValue() error {

	var i interface{}
	i, ok := <-p.Ch
	return i, ok
}

type PartitionTime struct {
	Ch chan time.Time
}

func (p *PartitionTime) CloseChannel() {
	close(p.Ch)
}

func (p *PartitionTime) ReadAllValues() chan interface{} {
	c := make(chan interface{})
	go func() {
		defer close(c)
		for v := range p.Ch {
			c <- v
		}
	}()
	return c
}

func (p *PartitionTime) ReadValue() (interface{}, bool) {
	var i interface{}
	i, ok := <-p.Ch
	return i, ok
}

type PartitionString struct {
	Ch chan string
}

func (p *PartitionString) CloseChannel() {
	close(p.Ch)
}

func (p *PartitionString) ReadAllValues() chan interface{} {
	c := make(chan interface{})
	go func() {
		defer close(c)
		for v := range p.Ch {
			c <- v
		}
	}()
	return c
}

func (p *PartitionString) ReadValue() (interface{}, bool) {
	var i interface{}
	i, ok := <-p.Ch
	return i, ok
}

type PartitionBool struct {
	Ch chan bool
}

func (p *PartitionBool) CloseChannel() {
	close(p.Ch)
}

func (p *PartitionBool) ReadAllValues() chan interface{} {
	c := make(chan interface{})
	go func() {
		defer close(c)
		for v := range p.Ch {
			c <- v
		}
	}()
	return c
}

func (p *PartitionBool) GetReadValue() (interface{}, bool) {
	var i interface{}
	i, ok := <-p.Ch
	return i, ok
}

type PartitionInterface struct {
	Ch chan interface{}
}

func (p *PartitionInterface) CloseChannel() {
	close(p.Ch)
}

func (p *PartitionInterface) ReadAllValues() chan interface{} {
	c := make(chan interface{})
	go func() {
		defer close(c)
		for v := range p.Ch {
			c <- v
		}
	}()
	return c
}

func (p *PartitionInterface) ReadValue() (interface{}, bool) {
	val, ok := <-p.Ch
	return val, ok
}

// type DHDColumn struct {
// 	ch                 interface{}
// 	primitiveType      string
// 	logicalType        string
// 	conversionFunction func(interface{}) interface{}
// }

// convertToTime converts time stored as milliseconds in an int64 to a time.Time type
func convertToTime(v interface{}) time.Time {
	var t time.Time
	if val, ok := v.(int64); ok {
		fmt.Printf("Time value: %d\n", val/1000)
		t = time.Unix(val/1000, 0)
	} else {
		log.Println("Error converting to time, invalid input type")
		os.Exit(1)
	}
	return t
}

func convertToFloat64(v interface{}) float64 {
	switch t := v.(type) {
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case float32:
		return float64(t)
	default:
		log.Println("Problem converting to float64")
		return 0
	}
}

func convertToString(v interface{}) string {
	var s string
	if val, ok := v.([]byte); ok {
		s = string(val)
	} else {
		log.Println("Error converting to string, invalid input type")
		os.Exit(1)
	}
	return s
}

// Ignore conversions for now to see if library does those for me
/*
func makeChannel(primitive string, logical *parquet.LogicalType) (interface{}, error) {
	switch primitive {
	case "BOOLEAN":
		return make(chan bool), nil
	case "INT32", "FLOAT", "DOUBLE":
		return make(chan float64), nil
	case "INT64":
		if logical != nil && logical.TIMESTAMP != nil {
			return make(chan time.Time), nil
		}
		return make(chan float64), nil
	case "BYTE_ARRAY":
		if logical != nil && logical.STRING != nil {
			return make(chan string), nil
		}
		return make(chan interface{}), nil
	default:
		return nil, errors.New("unsupported primitive or logical type provided")
	}
}
*/

// TODO: the numeric types need more checks and balances
func makePartition(primitive string, logical *parquet.LogicalType) (Partition, error) {
	switch primitive {
	case "BOOLEAN":
		return &PartitionBool{
			Ch: make(chan bool),
		}, nil
	case "INT32", "FLOAT", "DOUBLE":
		return &PartitionFloat64{
			Ch: make(chan float64),
		}, nil
	case "INT64":
		if logical != nil && logical.TIMESTAMP != nil {
			return &PartitionTime{
				Ch: make(chan time.Time),
			}, nil
		}
		return &PartitionFloat64{
			Ch: make(chan float64),
		}, nil
	case "BYTE_ARRAY":
		if logical != nil && logical.STRING != nil {
			return &PartitionString{
				Ch: make(chan string),
			}, nil
		}
		return &PartitionInterface{
			Ch: make(chan interface{}),
		}, nil
	default:
		return nil, errors.New("unsupported primitive or logical type provided")
	}
}

// ReadColumnFromParquet requires the file name and the column indices to be read
// returns the channels as interfaces for further interrogation
// nr is the number of rows to read. If nr is 0, all rows will be read (streamed)
// np is the number of rows to read at one time. If np is 0, 1000 rows will be read at one time
func ReadColumnsFromParquet(f string, cols []string, nr int64, chunkSize int64) ([]Partition, error) {
	// Open the file
	fr, err := local.NewLocalFileReader(f)
	if err != nil {
		log.Println("Can't open parquet file")
		return nil, err
	}
	defer fr.Close()

	// Create a column reader from the file
	pr, err := reader.NewParquetColumnReader(fr, numCPUs)
	if err != nil {
		log.Println("Can't create column reader from parquet file", err)
		return nil, err
	}
	defer pr.ReadStop()

	partitions := make([]Partition, 0)
	nf := make([]string, 0)

	for _, c := range cols {
		found := false
		for _, d := range pr.Footer.Schema {
			if d.GetName() == c {
				p, err := makePartition(d.GetType().String(), d.GetLogicalType())
				if err != nil {
					log.Println("Error creating channel for parquet column type")
					return nil, err
				}
				partitions = append(partitions, p)
				found = true
			}
		}
		if !found {
			nf = append(nf, c)
		}
	}

	// Ensure all columns were retrieved
	if len(nf) > 0 {
		log.Println("The following columns could not be retrieved from the parquet file: ", nf)
		return nil, fmt.Errorf("the following columns could not be retrieved: %v", nf)
	}

	totalRows := pr.GetNumRows()
	// Passing in 0 for nr means to return all of the rows
	// Passing in a number larger than the total rows available defaults to the total rows
	if nr == 0 || nr > totalRows {
		nr = totalRows
	}

	chunkSize = util.MinInt64(nr, totalRows, chunkSize, 1000) // only allow reading of at most 1,000 rows at a time

	// TODO: error-handling
	go func(pars []Partition, nr int64) {
		// Close the channels when streaming is complete
		for _, p := range pars {
			defer p.CloseChannel()
			// switch v := c.(type) {
			// case chan float64:
			// 	defer close(v)
			// case chan string:
			// 	defer close(v)
			// case chan bool:
			// 	defer close(v)
			// case chan interface{}:
			// 	defer close(v)
			// default:
			// 	log.Println("Invalid channel type provided, exiting application")
			// 	os.Exit(1) // TODO: better error-handling here
			// }
		}

		// Continue retrieving rows in batches from each column until we have reached the desired number of rows
		for i := int64(0); i < nr; {
			// Ensure we do not pull more values than required
			chunkSize = util.MinInt64(chunkSize, nr-i)
			// I don't need to call SkipRows each time, use this if I need to do an offset to start
			// err := pr.SkipRows(i)
			// if err != nil {
			// 	log.Println("Error offsetting parquet file")
			// 	break
			// }
			var wg sync.WaitGroup
			var numRowsRead int // TODO: will likely no longer need this check, can just use chunkSize
			l := len(cols)
			wg.Add(l)

			// Concurrent column reads
			// Sends all values for a column before sending for the next column
			for i := 0; i < l; i++ {
				// TODO: Error-handling here
				go func(p Partition, colName string, np int64) {
					// fmt.Println(colName)
					defer wg.Done()
					values, _, _, err := pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root."+colName), chunkSize)
					if err != nil {
						log.Println(err)
						return
					}
					numRowsRead = len(values)
					// Perform any conversions and send the value to the channel as the appropriate type
					for _, d := range values {
						// fmt.Println(reflect.TypeOf(d))
						switch pt := p.(type) {
						case *PartitionFloat64:
							converted := convertToFloat64(d)
							pt.Ch <- converted
						case *PartitionTime:
							converted := convertToTime(d)
							pt.Ch <- converted
						case *PartitionString:
							converted := convertToString(d)
							pt.Ch <- converted
						case *PartitionBool:
							pt.Ch <- d.(bool)
						case *PartitionInterface:
							pt.Ch <- d
						}
					}
				}(pars[i], cols[i], chunkSize)
			}
			wg.Wait() // wait for all channels to receive their chunk before moving on
			i += int64(numRowsRead)
			fmt.Printf("Rows offset: %d, rows read: %d, rows remaining: %d\n", i, numRowsRead, nr-i)
		}
		pr.ReadStop()
		fr.Close()
	}(partitions, nr)
	return partitions, nil
}

func ReadFromParquet(f string) {
	fr, err := local.NewLocalFileReader("output/flat.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	// pr, err := reader.NewParquetReader(fr, nil, 4)
	// if err != nil {
	// 	log.Println("Can't create parquet reader", err)
	// 	return
	// }

	pr, err := reader.NewParquetColumnReader(fr, numCPUs)
	// How to: pull information from schema for each field
	fmt.Println(pr.Footer.GetColumnOrders())
	fmt.Println(pr.Footer.ColumnOrders)
	fmt.Println(pr.Footer.IsSetColumnOrders()) // what does this mean??
	for _, d := range pr.Footer.Schema {
		fmt.Printf("ID: %d, Name: %s, Primitive Type: %s, Logical Type: %s\n", d.GetFieldID(), d.GetName(), d.GetType(), d.GetLogicalType())
	}

	if err != nil {
		log.Println("Can't create column reader", err)
		return
	}

	num := int(pr.GetNumRows())
	fmt.Println(num) // ~90,000 rows

	var numRowsToRead int64 = 10
	// rls = repetition levels
	// dls = definition levels
	// Read column by its name
	// transactionIDs, _, _, err := pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.TransactionID"), numRowsToRead)
	// Read column by its index
	transactionIDs, _, _, err := pr.ReadColumnByIndex(1, numRowsToRead)

	// I am going to have to check the types against my own that are saved somewhere?
	for _, id := range transactionIDs {
		fmt.Printf("%T\n", id)
		fmt.Println(id)
	}

	if err != nil {
		log.Println("Can't read", err)
		return
	}

	pr.ReadStop()
	fr.Close()

}

// r := reflect.TypeOf(c).Elem() // Will give me the type of the channel I am working with (e.g., int32)

/*
We can create a bunch of generator functions if it helps to return the appropriate types, it is essentially
a pipe used to perform the transformations. I might want to do this for my transformation anyway?

 func(interface{}) chan float64

 // What if I passed things around in a struct, with arrays of the channel types that are available and a map?

*/
