QUESTION: How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Answer: changing parameter value such as maxOffsetsPerTrigger can effect the throughput and latency. If we increase, each batch will process more data which will increase throughput but each batch will need more processing time to complete job.  

QUESTION: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Answer: the most 2 effective properties are
- maxOffsetsPerTrigger = 1000, although TODO specify that it should be 200, the processedRowPerSecond can be improved a lot if we increase the maxOffsetsPerTrigger > 200.
- psf.window(distinct_table.call_date_time,"1 days","1 hours"), avoid to use large windows size but very less  sliding interval because it will effect the processing time.   