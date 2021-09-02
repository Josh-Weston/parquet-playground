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

// TODO: In Go, there is no way to check if a channel has been closed
// will need better error-handling on SendValue to catch the panic() in case the channel is closed

// Generic information about a partition
type Partition interface {
	CloseChannel()
	ReadAllValues() chan interface{} // a convenience function for passing through all values from the channel
	ReadValue() (interface{}, bool)  // a convenience function for reading the value as an interface. (Dangerous) Must ensure no other operation is reading from the channel
	SendValue(interface{}) error     // a way to send a value on the channel
	IsClosed() bool
}

// The specific type of partition
type PartitionFloat64 struct {
	Ch     chan float64
	Closed bool
}

func (p *PartitionFloat64) CloseChannel() {
	if !p.Closed {
		close(p.Ch)
		p.Closed = true
	}
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

func (p *PartitionFloat64) ReadAllValuesTyped() chan float64 {
	return p.Ch
}

func (p *PartitionFloat64) GetChannel() (chan float64, error) {
	if p.Closed {
		return nil, errors.New("channel is closed")
	}
	return p.Ch, nil
}

func (p *PartitionFloat64) ReadValue() (interface{}, bool) {
	var i interface{}
	i, ok := <-p.Ch
	return i, ok
}

func (p *PartitionFloat64) ReadValueTyped() (float64, bool) {
	f, ok := <-p.Ch
	return f, ok
}

func (p *PartitionFloat64) SendValue(val interface{}) error {
	v, ok := val.(float64)
	if !ok {
		return fmt.Errorf("invalid type: expected float64, received %T", val)
	}
	p.Ch <- v
	return nil
}

func (p *PartitionFloat64) SendValueTyped(v float64) error {
	if p.Closed {
		return errors.New("channel is closed")
	}
	p.Ch <- v
	return nil
}

func (p *PartitionFloat64) IsClosed() bool {
	return p.Closed
}

type PartitionTime struct {
	Ch     chan time.Time
	Closed bool
}

func (p *PartitionTime) CloseChannel() {
	if !p.Closed {
		close(p.Ch)
		p.Closed = true
	}
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

func (p *PartitionTime) SendValue(val interface{}) error {
	v, ok := val.(time.Time)
	if !ok {
		return fmt.Errorf("invalid type: expected time.Time, received %T", val)
	}
	p.Ch <- v
	return nil
}

func (p *PartitionTime) IsClosed() bool {
	return p.Closed
}

type PartitionString struct {
	Ch     chan string
	Closed bool
}

func (p *PartitionString) CloseChannel() {
	if !p.Closed {
		close(p.Ch)
		p.Closed = true
	}
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

func (p *PartitionString) SendValue(val interface{}) error {
	v, ok := val.(string)
	if !ok {
		return fmt.Errorf("invalid type: expected string, received %T", val)
	}
	p.Ch <- v
	return nil
}

func (p *PartitionString) IsClosed() bool {
	return p.Closed
}

type PartitionBool struct {
	Ch     chan bool
	Closed bool
}

func (p *PartitionBool) CloseChannel() {
	if !p.Closed {
		close(p.Ch)
		p.Closed = true
	}
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

func (p *PartitionBool) ReadValue() (interface{}, bool) {
	var i interface{}
	i, ok := <-p.Ch
	return i, ok
}

func (p *PartitionBool) SendValue(val interface{}) error {
	v, ok := val.(bool)
	if !ok {
		return fmt.Errorf("invalid type: expected bool, received %T", val)
	}
	p.Ch <- v
	return nil
}

func (p *PartitionBool) IsClosed() bool {
	return p.Closed
}

type PartitionInterface struct {
	Ch     chan interface{}
	Closed bool
}

func (p *PartitionInterface) CloseChannel() {
	if !p.Closed {
		close(p.Ch)
		p.Closed = true
	}
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

func (p *PartitionInterface) SendValue(val interface{}) error {
	p.Ch <- val
	return nil
}

func (p *PartitionInterface) IsClosed() bool {
	return p.Closed
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

// NewPartitionFloat64 is a convenience function for creating a new PartitionFloat64
func NewPartitionFloat64() *PartitionFloat64 {
	return &PartitionFloat64{
		Ch: make(chan float64),
	}
}

// NewPartitionTime is a convenience function for creating a new PartitionTime
func NewPartitionTime() *PartitionTime {
	return &PartitionTime{
		Ch: make(chan time.Time),
	}
}

// NewPartitionBool is a convenience function for creating a new PartitionBool
func NewPartitionBool() *PartitionBool {
	return &PartitionBool{
		Ch: make(chan bool),
	}
}

// NewPartitionString is a convenience function for creating a new PartitionString
func NewPartitionString() *PartitionString {
	return &PartitionString{
		Ch: make(chan string),
	}
}

// NewPartitionInterface is a convenience function for creating a new PartitionInterface
func NewPartitionInterface() *PartitionInterface {
	return &PartitionInterface{
		Ch: make(chan interface{}),
	}
}

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
			l := len(cols)
			wg.Add(l)

			// Concurrent column reads
			// Sends all values for a column before sending for the next column
			// pr.ReadColumnByPath changes its internal state which can cause race conditions if we do not guard the operation with a lock
			var mu sync.Mutex
			for i := 0; i < l; i++ {
				// TODO: Error-handling here
				go func(p Partition, colName string, np int64, mu *sync.Mutex) {
					// fmt.Println(colName)
					defer wg.Done()
					mu.Lock()
					values, _, _, readErr := pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root."+colName), chunkSize)
					mu.Unlock()
					if readErr != nil {
						log.Println(readErr)
						return
					}
					// Perform any conversions and send the value to the channel as the appropriate type if the channel is still open
					for _, d := range values {
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
				}(pars[i], cols[i], chunkSize, &mu)
			}
			wg.Wait() // wait for all channels to receive their chunk before moving on
			i += int64(chunkSize)
			// fmt.Printf("Rows offset: %d, rows read: %d, rows remaining: %d\n", i, chunkSize, nr-i)
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

 // In DataFlow Engines it is common to pass a record around, not like I am doing.

*/
