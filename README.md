# How it works

The tricky part is we need to be streaming a reading from the values at all times or else we cause a deadlock.
So, we cannot pull columns unless we are going to be reading them. As this starts getting put in place I imagine
it will become more clear. The entire system requires that the values are being streamed into something (e.g., an http
endpoint or a file). This way, we can curtail/delay OOM (out of memory) issues.

One problem I have right now is you have to read all of the values from the channel before you close it. A channel can
only be closed once, attempting close a closed channel panics.

# Actions and Transformations

All actions and transformations return partitions, but the partitions are determined by the type of the operation.
We have three operations available:

    - Operations that work on all partitions (e.g., filter and aggregates)
	- Operations that work on a single partition (e.g., a mapping function)
	- Operations that create a new partition (e.g., a new calculated field)

# DHD (DataHook DataSet)

A DataHook DataSet is made-up of partitions. Partitions are simply columnar values. The order of the columns is not important,
but the order of the values in the columns are because when combined horizontally they form a row/record in the underlying
DataSource

# TODO

- Pretty print partition (head)