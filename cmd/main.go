package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	p "github.com/josh-weston/parquet-playground/internal/parquet"
	t "github.com/josh-weston/parquet-playground/internal/transform"
)

// TODO: Might want my own partition type to simplify these operations, do a DHD will consist of
// a slice of partitions, and the partitions will have their own properties to inspect

// top returns the first n rows from the partition
// func top(p interface{}) interface{} {

// }

// columnOrder

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// p.ReadFromParquet("output/flat.parquet")

	// At this point, I have the channels, they are properly typed, but they exist in an slice of interface{} (will need to be a DHD)
	partitions, err := p.ReadColumnsFromParquet("output/flat.parquet", []string{"TransactionID", "ProductID", "ReferenceOrderID", "ModifiedDate"}, 100, 15)
	if err != nil {
		log.Println("Error returned from ReadColumnsFromParquet")
		log.Println(err)
		os.Exit(1)
	}

	/**********
	FILTER TEST
	***********/

	// Operation will check if the first partition (index 0) is less than the constant value of 100
	o := t.Condition{
		PartitionIndex:  0,
		Operation:       t.Lte,
		ConstantValue:   100,
		ComparisonIndex: nil,
	}

	// Operation will check if the second partition (index 1) is less than the third partition (index 2)
	o2 := t.Condition{
		PartitionIndex:  1,
		Operation:       t.Gt,
		ConstantValue:   nil,
		ComparisonIndex: 2,
	}

	// Operation will check if the time partition occurs before May 30, 2011
	o3 := t.Condition{
		PartitionIndex:  3,
		Operation:       t.Lte,
		ConstantValue:   time.Date(2011, 05, 30, 0, 0, 0, 0, time.UTC),
		ComparisonIndex: nil,
	}

	filteredPartitions, err := t.Filter(partitions, o, o2, o3)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	/********
	TAKE TEST
	*********/
	takePartitions, err := t.Take(filteredPartitions, 50)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	const padding = 3
	w := tabwriter.NewWriter(os.Stdout, 0, 0, padding, ' ', 0) // 0 = align left

	// Our output header
	var s string = "\n"
	var u string = ""
	s += "\t"
	u += "\t"
	for i := range takePartitions {
		s += fmt.Sprintf("Col-%d\t", i+1)
		u += "----------\t"
	}
	fmt.Fprintln(w, s)
	fmt.Fprintln(w, u)

	// Our output values (cast to string)
	values := make([]string, len(takePartitions)+1) // add one for when we show the surrogate row index
	count := 0
	// Read my taken values
	for v := range takePartitions[0].ReadAllValues() {
		count++
		values[0] = fmt.Sprintf("[%d]", count)
		values[1] = fmt.Sprint(v)
		var wg sync.WaitGroup
		wg.Add(len(takePartitions) - 1)
		for i, l := 1, len(takePartitions); i < l; i++ {
			go func(p p.Partition, index int) {
				val, _ := p.ReadValue() // read value as an interface
				values[index+1] = fmt.Sprint(val)
				wg.Done()
			}(takePartitions[i], i)
		}
		wg.Wait() // wait for all channels to receive before writing to the console
		fmt.Fprintln(w, strings.Join(values, "\t"))
	}
	fmt.Fprintln(w)
	w.Flush()

	/*
		var wg sync.WaitGroup
		// multiplexStream := make(chan float64)
		// defer close(multiplexStream)
		multiplex := func(p *p.PartitionFloat64, i int) {
			defer wg.Done()
			for val := range p.Ch {
				fmt.Printf("Column Index: %d, Value: %.0f\n", i, val)
			}
		}

		// Cast to proper for easier access
		// chTyped := make([]chan float64, 0)
		// for _, c := range ch {
		// 	v, ok := c.(chan float64)
		// 	if !ok {
		// 		log.Println("We have a problem here")
		// 	} else {
		// 		chTyped = append(chTyped, v)
		// 	}
		// }


		// wg.Add(len(partitions))
		for i, par := range partitions {
			if par64, ok := par.(*p.PartitionFloat64); ok {
				wg.Add(1)
				go multiplex(par64, i)
			}

			if parTime, ok := par.(*p.PartitionTime); ok {
				for time := range parTime.Ch {
					fmt.Printf("ModifiedDate: %s\n", time.String())
				}
			}

			// Would want a for-select here if I need to operate on the values at the same time (e.g., an add operation)
		}

		wg.Wait()
	*/
	// At this point, we have a number of channels from the column indexes. We don't know what the column types
	// are, so we can't pass them around other than as interfaces (at this point)

	// 1) Keep passing them around as interfaces
	// 2) Cast them and send them on their way

	// addStream, err := a.AddInt32(ch...)
	// if err != nil {
	// 	log.Println(err)
	// 	os.Exit(1)
	// }

}

// The design pattern we are likely looking for here

/*

// fanIn runs multiple goroutines and pushes their output onto a single channel
fanIn := func(finders ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	multiplexStream := make(chan int)
	multiplex := func(finder <-chan int) {
		defer wg.Done()
		for primeNumber := range finder {
			multiplexStream <- primeNumber
		}
	}
	wg.Add(len(finders))
	for _, c := range finders {
		go multiplex(c)
	}
	go func() {
		wg.Wait()
		close(multiplexStream)
	}()
	return multiplexStream
}

*/
