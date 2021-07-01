# Approximate Nearest Airport

The objective of this code is to find the nearest airport(possibly best) available to user based on user's coordinates and 
airport's coordinates

## How to Run

1. Run Tests

`test` (from sbt-shell) or `sbt "test"` (from terminal)

2. Build the project

`clean;assembly` (from sbt-shell) or `sbt "clean;assembly"` (from terminal)

2. Run the application

```bash

java -jar target/scala-2.12/nearest-airport-locator.jar --input-files "/path-to/optd-airports-sample.csv,/path-to/user-geo-sample.csv" --output-file "/path-to/nearest_airport/"
```

## Implementation

### Infrastructure

The implementation is on Apache Spark as it is known for handling huge volumes of data(or events with spark streaming) in a 
distributed manner with resilience. This can be also be implemented in Apache Beam/Flink data streams but as I have 
already worked on spark, I went with that.

### Assumptions

 - Airport coordinates might contain invalid latitudes and longitudes.
 - The given dataset is the complete set of coordinates and up-to-date.
 - A maximum difference in distance to the nearest airport is 50 Kms.

### Logic

The logic is as follows,

 - Load Airport coordinates as a dataframe and sent to  broadcast for lookup operations.
 - Load user coordinates as dataframe and using a UDF to calculate the distance between coordinates.
 - Do a search on the airport coordinates to narrow down by fetching the areas using the latitude and longitude upper 
   and lower limits. This gives us the possible nearby airports. The search happens in two levels, 
   one with the latitude initially and with the longitudes next. Once we have the nearby airports, we use 
   Haversine Formula to calculate the distance between the points from the list of nearby airports and reduce the output
   to the nearest airport.
 - The above operation will be performed by UDF and for each user event in the dataset.

### Haversine vs other formulae

I looked over the internet for the best method to calculate the distance between two points. 
There are quite a few distance calculating methods like Euclidean, Manhattan etc but for this use-case Haversine might be
more suitable because Haversine method gives the distance based on the great-circle along the surface of
the earth and for instance Euclidean method gives the distance based on straight-line.

### Design for production 

Currently, the airport coordinates file is loaded and cleansed inside the same application. But for real-life production
scenarios, I would have a nightly job that updates the master Airport coordinates with new coordinates(if there are), 
clean the data and store the dataset. Then have a scheduled executor(for e.g., akka actor) inside the spark application to
refresh the table/file and re-broadcast the airport Coordinates.

## Trade-offs/Limitations

- As of today, spark support objects of size up to 8Gb to [broadcast](https://github.com/apache/spark/blob/79c66894296840cc4a5bf6c8718ecfd2b08bcca8/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L104). 
  So, we can safely assume that the master airport coordinates will not exceed even if there are new airports and 
  provided the metadata stays the same.
  But this also comes at the cost of choosing between the computing cost and efficiency (if we need to go with the 
  sql joins which can also become overly complicated in terms of correctness and maintenance)
- Due to time limitation, am not able to calculate the error margin in distance calculation and could not precisely
  test the logic in unit tests.

