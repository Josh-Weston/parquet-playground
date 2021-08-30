package transform

import (
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
		case 0:
			return s == v
		case 1:
			return s != v
		case 2:
			return s < v
		case 3:
			return s <= v
		case 4:
			return s > v
		case 5:
			return s >= v
		default:
			return false
		}
	case float64:
		f, ok := compVal.(float64)
		if !ok {
			return false
		}
		switch o {
		case 0:
			return f == v
		case 1:
			return f != v
		case 2:
			return f < v
		case 3:
			return f <= v
		case 4:
			return f > v
		case 5:
			return f >= v
		default:
			return false
		}
	case bool:
		b, ok := compVal.(bool)
		if !ok {
			return false
		}
		switch o {
		case 0:
			return b == v
		case 1:
			return b != v
		default:
			return false
		}
	case time.Time:
		t, ok := compVal.(time.Time)
		if !ok {
			return false
		}
		switch o {
		case 0:
			return t == v
		case 1:
			return t != v
		case 2:
			return t.Before(v)
		case 3:
			return t == v || t.Before(v)
		case 4:
			return t.After(v)
		case 5:
			return t == v || t.After(v)
		default:
			return false
		}
	default:
		return false // filter-out all if there is no comparison
	}
}

// This will need to be more robust for compound types
type Condition struct {
	ParitionIndex   int
	Operation       Operation
	ConstantValue   interface{} // check if this is nil first
	ComparisonIndex interface{} // check if this is nil second
}

type Operation int

const (
	eq Operation = iota
	neq
	lt
	lte
	gt
	gte
)

func (o Operation) String() string {
	switch o {
	case eq:
		return "equal"
	case neq:
		return "not equal"
	case lt:
		return "less than"
	case lte:
		return "less than or equal to"
	case gt:
		return "greater than"
	case gte:
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
func Filter(pars []p.Partition, conditions []Condition) ([]p.Partition, error) {

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
					values[index], _ = p.ReadValue()
					wg.Done()
				}(pars[i], i)
			}
			wg.Wait() // wait for all channels to receive before moving on

			// Check the conditions
			for _, c := range conditions {
				var result bool
				if c.ConstantValue != nil {
					result = CheckCondition(values[c.ParitionIndex], c.ConstantValue, c.Operation)
				} else if c.ComparisonIndex != nil {
					i, ok := c.ComparisonIndex.(int)
					if ok {
						result = CheckCondition(values[c.ParitionIndex], values[i], c.Operation)
					}
				}
				// If the condition is true, we pipe the values through our channels
				if result == true {
					var wg sync.WaitGroup
					wg.Add(len(filteredPars))
					for i := 0; i < len(filteredPars); i++ {
						go func(index int) {
							// Send value on the filtered par
							filteredPars.
						}(i)
					}
					wg.Wait()
				}
			}

			/*

			// This will need to be more robust for compound types
type Condition struct {
	ParitionIndex   int
	Operation       Operation
	ConstantValue   interface{} // check if this is nil first
	ComparisonIndex interface{} // check if this is nil second
}

			*/


		}
	}()

	return filteredPars, nil

	// if constant, no problem
	// if value from other channel, then store them in a slice

	// how to handle multiples?

	// first-loop through the conditions to see if any are dependent on each other, if not, then we don't need to do
	// any comparisons across conditions

}

// Select chooses the columns to make available
// TODO: closing the channels can be optimized before reaching select.
/*
func Select() {

}
*/

// I need someway of passing the filters through
