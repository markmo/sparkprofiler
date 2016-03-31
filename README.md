# sparkprofiler

Given the path to a Parquet file, compute the following statistics for each column,
and return as a JSON message:

* Highest value
* Lowest value
* Distinct values count
* Top 5 values and frequency
* Sum
* Row count
* Blank count
* Null count

Dependencies:

* io.metamorphic:analysis-commons:1.0

### TODO

* Make use of DataFrame functions such as `describe` and `freqItems`
