# Big Data Benchmarking Suite for Cyber-security Analytics

## Getting Started

### Prerequisites

* [Java SDK](https://www.java.com/) - Version 1.8
* [Scala](https://www.scala-lang.org/) - Version 2.11.8
* [Apache Spark](https://spark.apache.org/) - Version 2.1.0
* [Maven](https://maven.apache.org/) - Version 3.3.9

### Compiling

```
mvn -f csb/pom.xml package
```

## Running

The execution of the application is split in two different phases:
1. Analysis of the input dataset and generation of the seed graph and the probability distributions of the connections properties 
2. Data generation with either Barabási–Albert or Kronecker algorithms

### Running phase 1

```
$SPARK_HOME/bin/spark-submit csb/target/csb-dep.jar gen_dist data/dataset_01/conn.log data/dataset_01/alert aug.log seed_vert seed_edges
```

The output will be:
* An *aug.log* file which represents the join between the input log files
* A set of *.ser* files which represent the probability distributions of the connections properties
* *seed_vert* and *seed_edges* files which are the textual representation of the vertices and edges of the seed graph

### Running phase 2

The phase 2 is composed of three separate steps:
1. Synthetic graph generation, which is done using either one the algorithms listed below
2. Properties generation, which can be skipped with the *--no-prop* parameter
3. Graph saving using Spark serialization methods (Neo4j serialization is under development)
4. Veracity computation of degree, in-degree, out-degree and PageRank metrics

#### Barabási–Albert

The following will run the Barabási–Albert algorithm with 1000 iterations:

```
$SPARK_HOME/bin/spark-submit csb/target/csb-dep.jar ba seed_vert seed_edges 100
```

#### Kronecker
**Note:** the KronFit algorithm is currently under development, so a static version of the seed matrix (*seed.mtx*) of the provided dataset is included in the same folder.

The following will run the stochastic Kronecker algorithm with 15 iterations using the *seed.mtx* matrix:

```
$SPARK_HOME/bin/spark-submit csb/target/csb-dep.jar kro data/dataset_01/seed.mtx seed_vert seed_edges 10
```
