package action

/*
	Parquet Primitive		Go Type
	-----------------		-------
	BOOLEAN					bool
	INT32					int32
	INT64					int64
	INT96					string
	FLOAT					float32
	DOUBLE					float64
	BYTE_ARRAY				string
	FIXED_LEN_BYTE_ARRAY	string
*/

import (
	"math"

	p "github.com/josh-weston/parquet-playground/internal/parquet"
)

type Action interface {
	Cancel()
}

// Power receives a partition and returns the partition to the power of n
func Power(par *p.PartitionFloat64, n float64) (*p.PartitionFloat64, error) {
	newPartition := &p.PartitionFloat64{Ch: make(chan float64)}
	// TODO: error handling here
	// Read each value from each channel to preserve the order of rows
	go func(stream chan float64, p chan float64, n float64) {
		defer close(stream)
		for val := range p {
			result := math.Pow(val, n)
			stream <- result
		}
	}(newPartition.Ch, par.Ch, n)
	return newPartition, nil
}
