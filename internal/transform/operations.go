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
		case Eq:
			return s == v
		case Neq:
			return s != v
		case Lt:
			return s < v
		case Lte:
			return s <= v
		case Gt:
			return s > v
		case Gte:
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
		case Eq:
			return f == v
		case Neq:
			return f != v
		case Lt:
			return f < v
		case Lte:
			return f <= v
		case Gt:
			return f > v
		case Gte:
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
		case Eq:
			return b == v
		case Neq:
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
		case Eq:
			return t == v
		case Neq:
			return t != v
		case Lt:
			return t.Before(v)
		case Lte:
			return t == v || t.Before(v)
		case Gt:
			return t.After(v)
		case Gte:
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
			filteredPars[i] = &p.PartitionBool{}
		case *p.PartitionString:
			filteredPars[i] = &p.PartitionString{}
		case *p.PartitionFloat64:
			filteredPars[i] = &p.PartitionFloat64{}
		case *p.PartitionTime:
			filteredPars[i] = &p.PartitionTime{}
		case *p.PartitionInterface:
			filteredPars[i] = &p.PartitionInterface{}
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
// Take will need to drain or close the old channel, or else it will hang
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
// TODO: closing the channels can be optimized before reaching select.
/*
func Select() {

}
*/

// I need someway of passing the filters through
