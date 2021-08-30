package parquet

import (
	"log"
	"runtime"
)

var numCPUs int64

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	numCPUs = int64(runtime.NumCPU())
}
