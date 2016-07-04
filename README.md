# weatherdata

Description
-----------

Generate test weather data using the `spark-ts` time series library for Apache Spark.
731 days of historical data of 5 New Zealand weather stations is consumed and 731 more days are predicted.

Minimum Requirements
--------------------

* Java 1.8
* Maven 3.1
* Apache Spark 1.6.1
* Scala 2.11.8

Using this Repo
---------------

### Building

Use [Maven](https://maven.apache.org/) for building. To compile and build:

    mvn package

### Running

To submit to a local Spark cluster, run the following command
from the `weatherdata` directory:

    spark-submit --class com.ebon.Workflow target/weatherdata-1.0-SNAPSHOT-jar-with-dependencies.jar