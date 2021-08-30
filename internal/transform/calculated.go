package transform

import (
	"errors"
	"sync"
)

// AddInt32 adds two or more Int32 columns together
// returns a channel with the results of the add operation and an error (if any)
// TODO: this requires a timeout/done channel to avoid hanging when one of the channels fails
func Add(pars ...chan float64) (chan float64, error) {
	ch := make(chan float64)
	defer close(ch)

	// Must supply at least two columns
	if len(pars) < 2 {
		return nil, errors.New("must provide at least two columns to perform an add operation")
	}

	// TODO: error handling here
	// Read each value from each channel to preserve the order of rows
	go func(addStream chan float64) {
		for val := range pars[0] {
			// Wait for each channel to provide its value before continuing
			var wg sync.WaitGroup
			l := len(pars)
			wg.Add(l)
			result := val
			for i := 0; i < l; i++ {
				go func(c <-chan float64) {
					result += <-c
					wg.Done()
				}(pars[i])
			}
			wg.Wait() // wait for all channels to receive before moving on
			addStream <- result
		}
	}(ch)
	return ch, nil
}
