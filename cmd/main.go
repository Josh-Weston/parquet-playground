package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"text/tabwriter"

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

	/*
		// Operation will check if the first partition (index 0) is less than the second partition (index 1)
		o := t.Condition{
			ParitionIndex:   0,
			Operation:       t.Eq,
			ConstantValue:   nil,
			ComparisonIndex: 1,
		}

		filteredPartitions, err := t.Filter(partitions, o)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
	*/

	/********
	TAKE TEST
	*********/
	takePartitions, err := t.Take(partitions, 50)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	const padding = 3
	w := tabwriter.NewWriter(os.Stdout, 0, 0, padding, ' ', 0) // 0 = align left

	// Our output header
	var s string = "\n"
	var u string = ""
	for i := range takePartitions {
		s += fmt.Sprintf("Col-%d\t", i+1)
		u += "----------\t"
	}
	fmt.Fprintln(w, s)
	fmt.Fprintln(w, u)

	// Our output values (cast to string)
	values := make([]string, len(takePartitions))
	// Read my taken values
	for v := range takePartitions[0].ReadAllValues() {
		values[0] = fmt.Sprint(v)
		var wg sync.WaitGroup
		wg.Add(len(takePartitions) - 1)
		for i := 1; i < len(takePartitions); i++ {
			go func(p p.Partition, index int) {
				val, _ := p.ReadValue() // read value as an interface
				values[index] = fmt.Sprint(val)
				wg.Done()
			}(takePartitions[i], i)
		}
		wg.Wait() // wait for all channels to receive before moving on
		// Now print our values to the console
		fmt.Fprintln(w, strings.Join(values, "\t"))
	}
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
