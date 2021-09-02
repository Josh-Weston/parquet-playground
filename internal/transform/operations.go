package transform

import (
	"errors"
	"log"
	"sync"
	"time"

	p "github.com/josh-weston/parquet-playground/internal/parquet"
)

// TODO: need error-handling here
// Returning true means to keep the value, returning false means to remove the value
func CheckCondition(val interface{}, compVal interface{}, o Operation) bool {
	switch v := val.(type) {
	case string:
		s, ok := compVal.(string)
		if !ok {
			return false
		}
		switch o {
		case Eq:
			return v == s
		case Neq:
			return v != s
		case Lt:
			return v < s
		case Lte:
			return v <= s
		case Gt:
			return v > s
		case Gte:
			return v >= s
		default:
			return false
		}
	case float64:
		var f float64
		switch t := compVal.(type) {
		case float64:
			f = t
		case int:
			f = float64(t)
		case int8:
			f = float64(t)
		case int16:
			f = float64(t)
		case int32:
			f = float64(t)
		case int64:
			f = float64(t)
		default:
			return false
		}
		switch o {
		case Eq:
			return v == f
		case Neq:
			return v != f
		case Lt:
			return v < f
		case Lte:
			return v <= f
		case Gt:
			return v > f
		case Gte:
			return v >= f
		default:
			return false
		}
	case bool:
		b, ok := compVal.(bool)
		if !ok {
			return false
		}
		switch o {
		case Eq:
			return v == b
		case Neq:
			return v != b
		default:
			return false
		}
	case time.Time:
		t, ok := compVal.(time.Time)
		if !ok {
			return false
		}
		switch o {
		case Eq:
			return v == t
		case Neq:
			return v != t
		case Lt:
			return v.Before(t)
		case Lte:
			return v == t || v.Before(t)
		case Gt:
			return v.After(t)
		case Gte:
			return v == t || v.After(t)
		default:
			return false
		}
	default:
		return false // filter-out all if there is no comparison
	}
}

// This will need to be more robust for compound types
type Condition struct {
	PartitionIndex  int
	Operation       Operation
	ConstantValue   interface{} // check if this is nil first
	ComparisonIndex interface{} // check if this is nil second
}

type Operation int

const (
	Eq Operation = iota
	Neq
	Lt
	Lte
	Gt
	Gte
)

func (o Operation) String() string {
	switch o {
	case Eq:
		return "equal"
	case Neq:
		return "not equal"
	case Lt:
		return "less than"
	case Lte:
		return "less than or equal to"
	case Gt:
		return "greater than"
	case Gte:
		return "greater than or equal to"
	default:
		return "<unknown operation>"
	}
}

// Operations can only be applied to channels, not to some calculation. The calculations will need to have been done
// prior to this, we can optimize this later (e.g., function chaining or something). We could do this by having a wrapper
// channel that applies multiple functions to a single value and produces a channel of the same type.
// Have a wrapper around functional work that takes in a channel and spits-out a channel

// Filter returns the same number of partitions, but only the values that satisfy the predicate are
// pushed to the partition
func Filter(pars []p.Partition, conditions ...Condition) ([]p.Partition, error) {

	// Nothing to do if the partitions or condition are empty
	if len(pars) == 0 || len(conditions) == 0 {
		return pars, nil
	}

	filteredPars := make([]p.Partition, len(pars))

	// spin-up our new channels
	// TODO: This will likely be a popular function for me
	for i, par := range pars {
		switch par.(type) {
		case *p.PartitionBool:
			filteredPars[i] = &p.PartitionBool{Ch: make(chan bool)}
		case *p.PartitionString:
			filteredPars[i] = &p.PartitionString{Ch: make(chan string)}
		case *p.PartitionFloat64:
			filteredPars[i] = &p.PartitionFloat64{Ch: make(chan float64)}
		case *p.PartitionTime:
			filteredPars[i] = &p.PartitionTime{Ch: make(chan time.Time)}
		case *p.PartitionInterface:
			filteredPars[i] = &p.PartitionInterface{Ch: make(chan interface{})}
		}
	}

	go func() {
		values := make([]interface{}, len(filteredPars))
		for _, p := range filteredPars {
			defer p.CloseChannel()
		}

		for v := range pars[0].ReadAllValues() {
			values[0] = v
			var wg sync.WaitGroup
			wg.Add(len(pars) - 1)
			for i := 1; i < len(pars); i++ {
				go func(p p.Partition, index int) {
					values[index], _ = p.ReadValue() // read value as an interface
					wg.Done()
				}(pars[i], i)
			}
			wg.Wait() // wait for all channels to receive before moving on

			result := true
			// Check the conditions
			for _, c := range conditions {
				if !result {
					break // short-circuit if one of the conditions has already failed
				}
				// TODO: some way to notify the user they are missing conditions or their indices are out of range.
				// We don't stop the operation, we simply ignore the condition
				if (c.ConstantValue == nil && c.ComparisonIndex == nil) || c.PartitionIndex >= len(values) {
					continue
				}
				if c.PartitionIndex > len(values) {
					continue
				}

				if c.ConstantValue != nil {
					result = CheckCondition(values[c.PartitionIndex], c.ConstantValue, c.Operation)
				} else if c.ComparisonIndex != nil {
					i, ok := c.ComparisonIndex.(int)
					if ok && i < len(values) { // ignore condition if ComparisonIndex is out of range
						result = CheckCondition(values[c.PartitionIndex], values[i], c.Operation)
					}
				}
			}
			// If the conditions evaluate to true, we pipe the values through our channels
			if result {
				var wg sync.WaitGroup
				wg.Add(len(filteredPars))
				for i := 0; i < len(filteredPars); i++ {
					go func(idx int) {
						// Send value on the filtered par
						filteredPars[idx].SendValue(values[idx])
						wg.Done()
					}(i)
				}
				wg.Wait()
			}
		}
	}()

	return filteredPars, nil

	// if constant, no problem
	// if value from other channel, then store them in a slice

	// how to handle multiples?

	// first-loop through the conditions to see if any are dependent on each other, if not, then we don't need to do
	// any comparisons across conditions

}

// Take returns the top/head number of rows specified
func Take(pars []p.Partition, numRows int) ([]p.Partition, error) {
	// If value is not a natural number, we set it to 100 by default
	if numRows < 1 {
		numRows = 100
	}

	takePars := make([]p.Partition, len(pars))
	// spin-up our new channels
	// TODO: we might want to synchronize these better to ensure all partitions send the same number of rows. They
	// should implicitly send the same number of rows as they should all be the same shape all of the time.
	for i, l := 0, len(pars); i < l; i++ {
		switch parValue := pars[i].(type) {
		case *p.PartitionBool:
			par := &p.PartitionBool{Ch: make(chan bool)}
			takePars[i] = par
			go func(n int, v *p.PartitionBool) {
				defer par.CloseChannel()
				for val := range v.Ch {
					if n > 0 {
						par.Ch <- val
						n--
					}
				}
			}(numRows, parValue)
		case *p.PartitionString:
			par := &p.PartitionString{Ch: make(chan string)}
			takePars[i] = par
			go func(n int, v *p.PartitionString) {
				defer par.CloseChannel()
				for val := range v.Ch {
					if n > 0 {
						par.Ch <- val
						n--
					}
				}
			}(numRows, parValue)
		case *p.PartitionFloat64:
			par := &p.PartitionFloat64{Ch: make(chan float64)}
			takePars[i] = par
			go func(n int, v *p.PartitionFloat64) {
				defer par.CloseChannel()
				for val := range v.Ch {
					if n > 0 {
						par.Ch <- val
						n--
					}
				}
			}(numRows, parValue)
		case *p.PartitionTime:
			par := &p.PartitionTime{Ch: make(chan time.Time)}
			takePars[i] = par
			go func(n int, v *p.PartitionTime) {
				defer par.CloseChannel()
				for val := range v.Ch {
					if n > 0 {
						par.Ch <- val
						n--
					}
				}
			}(numRows, parValue)
		case *p.PartitionInterface:
			par := &p.PartitionInterface{Ch: make(chan interface{})}
			takePars[i] = par
			go func(n int, v *p.PartitionInterface) {
				defer par.CloseChannel()
				for val := range v.Ch {
					if n > 0 {
						par.Ch <- val
						n--
					}
				}
			}(numRows, parValue)
		}
	}
	return takePars, nil
}

// Select chooses the columns to make available
// TODO: closing the channels can be optimized before reaching select. The channels need to be drained to avoid a deadlock
// so they can either be closed or drained before reaching this step

// Select returns the selected partitions.
// This operation is usually called at the end, or after an operation that required a column that is no longer needed
// idx is the list of partitions to keep
// TODO: ability to select the same partition multiple times. This might be better suited as an "Alias" operator?
func Select(pars []p.Partition, idx []int) ([]p.Partition, error) {
	// Nothing to do if the partitions or indices are not specified
	if len(pars) == 0 || len(idx) == 0 {
		return pars, nil
	}

	// Determine number of valid indices to avoid reallocated memory
	validIdx := 0
	for _, j := range idx {
		if j < len(pars) {
			validIdx++
		}
	}

	if validIdx == 0 {
		return pars, errors.New("no valid indices provided, returning all columns")
	}

	selectedPars := make([]p.Partition, validIdx)

	// TODO: notify the originator that they can safely close the channel (e.g., it is no longer needed)
	// otherwise, I need to drain the channel like I am doing here
	for i := range pars {
		selected := false
		for j, index := range idx {
			if i == index {
				selectedPars[j] = pars[i] // so the columns can be re-organized
				selected = true
				break
			}
		}
		// Drain the channel if not selected to avoid a deadlock
		if !selected {
			go func(par p.Partition) {
				for range par.ReadAllValues() {
					// drain the channel
				}
			}(pars[i])
		}
	}
	return selectedPars, nil
}

// AddColumns returns the partitions with a new partition for the addition of the selected columns
// TODO: this needs to be generalize for additional calculations
func AddColumns(pars []p.Partition, idx []int) ([]p.Partition, error) {

	// Nothing to do if the partitions or indices are not specified
	if len(pars) == 0 || len(idx) <= 1 {
		return pars, errors.New("invalid indices provided")
	}

	valid := true
	for _, j := range idx {
		if j >= len(pars) || j < 0 {
			valid = false
		}
	}

	if !valid {
		return pars, errors.New("invalid indices provided")
	}

	calculatedPars := make([]p.Partition, len(pars)+1) // add 1 for the new partition
	valuePars := make([]*p.PartitionFloat64, len(idx)) // can only handle Float64 partitions for now
	passThroughIdx := make([]int, len(idx))            // need to pass-through the original values to the channels involved in the calculation
	currIdx := 0
	for i := range pars {
		selected := false
		for _, j := range idx {
			if i == j {
				selected = true
				switch t := pars[i].(type) {
				case *p.PartitionBool:
					calculatedPars[i] = p.NewPartitionBool()
				case *p.PartitionString:
					calculatedPars[i] = p.NewPartitionString()
				case *p.PartitionFloat64:
					valuePars[currIdx] = t
					passThroughIdx[currIdx] = i
					currIdx++
					calculatedPars[i] = p.NewPartitionFloat64()
				case *p.PartitionTime:
					calculatedPars[i] = p.NewPartitionTime()
				case *p.PartitionInterface:
					calculatedPars[i] = p.NewPartitionInterface()
				}
				break
			}
		}
		if !selected {
			calculatedPars[i] = pars[i]
		}
	}

	// Create my new channel for the calculation
	calculatedPar := p.NewPartitionFloat64()
	calculatedPars[len(calculatedPars)-1] = calculatedPar

	go func() {
		// Read the values from the partitions involved in the calculations
		values := make([]float64, len(valuePars)) // avoid the need for locking and synchronization

		// Close calculated channel and the passthrough channels once all values are read
		defer calculatedPar.CloseChannel()
		for _, idx := range passThroughIdx {
			defer calculatedPars[idx].CloseChannel()
		}

		// I need to pass the values through to the partitions that are involved in the calculations
		for v := range valuePars[0].ReadAllValuesTyped() {
			values[0] += v
			var wg sync.WaitGroup
			wg.Add(len(valuePars) - 1)
			for i := 1; i < len(valuePars); i++ {
				go func(p *p.PartitionFloat64, idx int) {
					values[idx], _ = p.ReadValueTyped()
					wg.Done()
				}(valuePars[i], i)
			}
			wg.Wait() // wait for all channels to receive before moving on
			var result float64
			for i := range values {
				result += values[i]
			}

			wg.Add(len(values) + 1) // calculated partition + the pass-through partitions
			// Send result to the calculated column
			go func() {
				// TODO: better error-handling here
				defer wg.Done()
				err := calculatedPar.SendValueTyped(result)
				if err != nil {
					log.Println(err)
				}
			}()

			// Pass-through original values to the partitions involved in the calculation
			for i, l := 0, len(values); i < l; i++ {
				go func(par p.Partition, v float64) {
					defer wg.Done()
					par.SendValue(v) // TODO: better error-handling here
				}(calculatedPars[passThroughIdx[i]], values[i])
			}
			wg.Wait()
		}
	}()

	return calculatedPars, nil
}

// How an aggregate function might work
/*

	// Read the values from the partitions involved in the calculations
	values := make([]float64, len(valuePars)) // avoid the need for locking and synchronization
	defer calculatedPar.CloseChannel()
	var wg sync.WaitGroup
	wg.Add(len(valuePars))
	for i, l := 0, len(valuePars); i < l; i++ {
		go func(p *p.PartitionFloat64, idx int) {
			// TODO: better error-handling here for when channel is closed
			defer wg.Done()
			ch, _ := p.GetChannel()
			for val := range ch {
				values[idx]+= val
			}
		}(valuePars[i], i)
	}
	wg.Wait()
	// Perform calculation

*/
