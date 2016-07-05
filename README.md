# weatherdata


Description
-----------

Generate test weather data using the `spark-ts` time series library for Apache Spark.
731 days of historical data of 5 New Zealand weather stations is consumed and 731 more days are predicted.
The result is then an output file will be names weathertestdata.txt located under /data/output.
 
 
Not implemented features 
------------------------

Not implemented due to reduced scope:
        1- First step of the `Worflow`  main class would have been 
        Using IDW Interpolation (`IdwInterpolation`) to predict climate data of 5 additional weather stations utilising the set of
        the 5 weather stations having historical data based on their geographical attributes:
               - latitude
               - longitude
               - altitude
        And then running the `TimeSeries` as second step.
        2- Letting the user pass as a argument the desired number of extra days to predict.


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

To submit to a local Spark cluster, run the following command from the project `weatherdata` main directory:

    spark-submit --class com.ebon.Workflow target/weatherdata-1.0-SNAPSHOT-jar-with-dependencies.jar