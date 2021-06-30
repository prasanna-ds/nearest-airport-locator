# Approximate Nearest Airport

## How to Run

1. Run Tests

`test` (from sbt-shell) or `sbt "test"` (from terminal)

2. Build the project

`clean;assembly` (from sbt-shell) or `sbt "clean;assembly"` (from terminal)

2. Run the application

```bash

java -jar target/scala-2.12/nearest-airport-finder.jar --input-files "/path-to/optd-airports-sample.csv,/path-to/user-geo-sample.csv" --output-file "/path-to/nearest_airport/"
```

## Trade-offs/Limitations


