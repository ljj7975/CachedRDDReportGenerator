# CachedRDDReportGenerator
Fall '17, University of Waterloo.

## Author
Jaejun (Brandon) Lee, j474lee@edu.uwaterloo.ca

## Background
In order to take advatnage of full functionality of Spark, maximizing use of cached RDD is very important
However, with current version of Spark, it is very challenging to analyze gain from cached RDD
CachedRDDReportGenerator is designed to provide a way of interpretting current use of cached RDD by generating an usage report using Spark Event Logs

## Build steps
Gradle is used as the build tool.

* Clone the repo: `git clone git@github.com:ljj7975/CachedRDDReportGenerator.git`
* To build project: `gradle build`

## Running CachedRDDReportGenerator
CachedRDDReportGenerator takes log file generated by Spark when Spark Spark Event log file:
* `java -jar build/libs/CachedRDDReportGenerator-1.0-SNAPSHOT.jar <SparkEventLogFile>`

If SparkEventLogFile argument is not specified, most recent log from default event log dir (/tmp/spark-events) will be used

## Detail about Usage Report
Usage report contains RDD id and 7 counters where each counter represents different use cases of the RDD as following:
M


### sample usage report
```
0 0 0 0 1 0 0 0 1
1 1 0 0 0 0 0 1 0
2 1 1 0 0 0 0 0 0
```

## Relevent Research
CachedRDDReportGenerator is developed as part of a research directed by Ken Salem (cs.uwaterloo.ca/~kmsalem/) with assistent of Michael Mior

In Spark, if RDD is expected to be used in near future, user can prevent recomputing same RDD by caching it.
However, eviction of cached RDD is not controlled by user and Spark simply follows LRU caching mechanism.
The main focus of the research is to see if we can design better caching mechanism that works better in Spark computing model by analyzing how LRU caching mechanism fits with Spark
