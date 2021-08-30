package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/denisenkom/go-mssqldb"
	p "github.com/josh-weston/parquet-playground/internal/parquet"
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

	// At this point, I have the channels, they are properly typed, but they exist in an slice of interface{}
	partitions, err := p.ReadColumnsFromParquet("output/flat.parquet", []string{"TransactionID", "ProductID", "ReferenceOrderID", "ModifiedDate"}, 100, 15)
	if err != nil {
		log.Println("Error returned from ReadColumnsFromParquet")
		log.Println(err)
		os.Exit(1)
	}

	/*
		Alright, so I have now received back my DHD, which is a bunch of typed channels, now what?
		We have three types:
			- Operations that work on all partitions (e.g., filter and aggregates)
			- Operations that work on a single partition (e.g., a mapping function)
			- Operations that create a new partition (e.g., a new calculated field)
	*/

	// I can't test much until this is more robust

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

/*
	Are we ever actually storing the data in-memory? There must be times when we will have to because of aggregations?



	The tricky part is we need to be streaming a reading from the values at all times or else we cause a deadlock.
	So, we cannot pull columns unless we are going to be reading them. As this starts getting put in place I imagine
	it will become more clear.


*/
