package action

import "math"

type Action interface {
	Cancel()
}

// Exponent receives a partition and returns the partition to the power of n
// TODO: these sorts of calculations should likely just be hand-written?
func POWER(par chan float64, n float64) (chan float64, error) {
	newPartition := make(chan float64)
	defer close(newPartition)
	// TODO: error handling here
	// Read each value from each channel to preserve the order of rows
	go func(stream chan float64, p chan float64, n float64) {
		for val := range par {
			result := math.Pow(val, n)
			stream <- result
		}
	}(newPartition, par, n)
	return newPartition, nil
}

// As a series of functions is so much easier to conceptualize

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
