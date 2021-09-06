package main

import (
	"log"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	a "github.com/josh-weston/parquet-playground/internal/action"
	p "github.com/josh-weston/parquet-playground/internal/parquet"
	t "github.com/josh-weston/parquet-playground/internal/transform"
)

// TODO: Might want my own partition type to simplify these operations, do a DHD will consist of
// a slice of partitions, and the partitions will have their own properties to inspect

// TODO: each time we run the pipeline we will need to give it a UUID to differentiate the snapshots

// top returns the first n rows from the partition
// func top(p interface{}) interface{} {

// }

// columnOrder

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// p.ReadFromParquet("output/flat.parquet")

	// At this point, I have the channels, they are properly typed, but they exist in an slice of interface{} (will need to be a DHD)
	partitions, err := p.ReadColumnsFromParquet("output/flat.parquet", []string{"TransactionID", "ProductID", "ReferenceOrderID", "ModifiedDate"}, 0, 100)
	if err != nil {
		log.Println("Error returned from ReadColumnsFromParquet")
		log.Println(err)
		os.Exit(1)
	}

	/**********
	ACTION TEST
	**********/
	// TODO: The ability to combine these actions
	actionedPartitions := make([]p.Partition, len(partitions)) // we need to create a new slice to avoid re-assigning to the old channel pointer, which causes a race condition
	idx := 0
	switch p := partitions[idx].(type) {
	case *p.PartitionFloat64:
		squaredPar, _ := a.Power(p, 2)
		for i := range partitions {
			if i == idx {
				actionedPartitions[i] = squaredPar
			} else {
				actionedPartitions[i] = partitions[i]
			}
		}
	}

	actionedPartitions2 := make([]p.Partition, len(actionedPartitions))
	idx = 2
	switch p := actionedPartitions[idx].(type) {
	case *p.PartitionFloat64:
		squaredPar, _ := a.Power(p, 4)
		for i := range actionedPartitions {
			if i == idx {
				actionedPartitions2[i] = squaredPar
			} else {
				actionedPartitions2[i] = actionedPartitions[i]
			}
		}
	}

	/**********
	FILTER TEST
	***********/

	// Operation will check if the first partition (index 0) is less than the constant value of 100
	o := t.Condition{
		PartitionIndex:  0,
		Operation:       t.Lte,
		ConstantValue:   500,
		ComparisonIndex: nil,
	}

	// Operation will check if the second partition (index 1) is less than the third partition (index 2)
	o2 := t.Condition{
		PartitionIndex:  1,
		Operation:       t.Lte,
		ConstantValue:   nil,
		ComparisonIndex: 2,
	}

	// Operation will check if the time partition occurs before May 30, 2011
	o3 := t.Condition{
		PartitionIndex:  3,
		Operation:       t.Lte,
		ConstantValue:   time.Date(2011, 06, 01, 0, 0, 0, 0, time.UTC),
		ComparisonIndex: nil,
	}

	filteredPartitions, err := t.Filter(actionedPartitions2, o, o2, o3)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	/*********************
	CALCULATED COLUMN TEST
	**********************/
	calculatedPartitions, err := t.AddColumns(filteredPartitions, []int{0, 1})
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	/************
	GROUPING TEST
	************/
	// TODO: Deadlock when more than 1 groupby specified
	groupedPartitions, err := t.GroupBy(calculatedPartitions, []int{3, 2}, []string{"SUM", "MAX", "AVG"})
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	/**********
	SELECT TEST
	***********/
	selectedPartitions, err := t.Select(groupedPartitions, []int{0, 1, 2, 3, 4})
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	/********
	TAKE TEST
	*********/
	takePartitions, err := t.Take(selectedPartitions, 50)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	/********
	TOP TEST
	*********/
	topPartitions, err := t.Top(takePartitions, 10, 4, 0)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	/**************
	VIEW PARTITIONS
	***************/
	t.Viewer(topPartitions)

	// TODO: The final output always needs to be a view or a streamed response to somewhere. For now just make it a view

}
