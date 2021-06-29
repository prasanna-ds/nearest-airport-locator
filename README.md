# Nth degree connections

[![Build Status](https://travis-ci.org/prasanna-ds/spark-nth-degree-connections.svg?branch=master)](https://travis-ci.org/prasanna-ds/spark-nth-degree-connections)

## How to Run

1. Run Tests

`test` (from sbt-shell) or `sbt "test"` (from terminal)

2. Build the project

`clean;assembly` (from sbt-shell) or `sbt "clean;assembly"` (from terminal)

2. Run the application

```bash

java -jar target/scala-2.12/calculate-nth-degree-connections.jar --input-file "/Users/prasanna/Documents/connnections.csv" --output-file "/Users/prasanna/Documents/connections/" --degrees 2
```

## Algorithm, Time and Space complexities

When run on a single machine, a solution can achieved using a hash table of edges and loop it for the value of N
by adding the already visited vertices.

```
1. create a hash table of swapped edges, lets say edges_hash_table
2. a list to manage traversed vertices vertices_list and add a starting vertex
3. a set to manage unique nth connections nth_conn_set
2. for each number in range(1 to Nthdegree)
     for each vertex in vertices_list
        for connection in edges_hash_table[vertex]
            if connection is not = vertices_list
                add connection to nth_conn_set
     add connection to vertices_list
   return nth_conn_set 
```

### Time Complexity
creating hash table with for loop -> n
Adding to list and looking up from hash tables(also in loops) -> (1 + 1+ ..)
outer for loop -> n
first inner for loop -> n
second inner for loop -> n

So, n + (1+1+..) + (n * n * n) and after dropping constants and lower order terms, 
complexity is o(n3) (worst-case) which is quite expensive for this solution if it runs locally, 
could be optimized using graph algorithms.

### Space Complexity
o(n) where n being the size of edges. 

## Technologies/Framework used

Though this a typical graph problem and not having worked on Graphx before, I chose to do it via Spark Dataframes and SQL operations.
The reason behind spark as choice of framework is that,
   -   It is natively a distributed computing framework and can scale horizontally.
   -   Has state-of-the-art query optimizer(catalyst), predicate push-downs and physical execution engine.


## Correctness of the solution

The solution scales when run in spark framework and regarding the correctness of the solution, there are unit tests in place
that verifies that all points of the requirement is met.


## Trade-offs/Limitations

The only limitation can happen I think is where I am making the tail recursion to calculate the nth degree of a connection, and they could be
 -   The recursion function will run on driver process (but still the dataframe operations should run on executors and not affect the performance.
        Also, no data is brought to the driver.)
 -   stack overflow error for the large value of N (a loop can also in place for the recursion).
Based on my experience so far, I have not used a recursion or loops in spark applications, but this is really an interesting thing to
check the performance for the large data and the large value of N.

