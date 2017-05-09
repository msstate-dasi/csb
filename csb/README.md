# CSB

## Getting Started

### Dependencies

* [Apache Maven](https://maven.apache.org/) - Version 3.3.9
* [Apache Spark](https://spark.apache.org/) - Version 2.1.0

### Building

```
mvn package
```

## Running

The execution of the application is split in two different phases:
1. Analysis of the input dataset and generation of the seed graph and the probability distributions of the connections properties 
2. Data generation with either Barabási–Albert or Kronecker algorithms

### Running phase 1

```
$SPARK_HOME/bin/spark-submit csb.jar seed -a $DATA/dataset_01/alert -b $DATA/dataset_01/conn.log
```

The output will be:
* An *aug.log* file which represents the join between the input log files
* A set of *.ser* files which represent the probability distributions of the connections properties
* *seed_vertices* and *seed_edges* folders which contain respectively the serialized representation of the vertices and edges of the seed graph

### Running phase 2

The phase 2 is composed of three separate steps:
1. Synthetic graph generation, which is done using either one the algorithms listed below
2. Properties generation, which can be skipped with the *-x* or the *--exclude-prop* options
3. Graph saving using Spark serialization methods (Neo4j serialization is under development)
4. Veracity computation of degree, in-degree, out-degree and PageRank metrics

#### Barabási–Albert

The following will run the Barabási–Albert algorithm with 10 iterations:

```
$SPARK_HOME/bin/spark-submit csb.jar synth ba -m all 10 0.2
```

#### Kronecker
**Note:** the KronFit algorithm is currently under development, so a static version of the seed matrix (*seed.mtx*) of the provided dataset is included in the same folder.

The following will run the stochastic Kronecker algorithm with 15 iterations using the *seed.mtx* matrix:

```
$SPARK_HOME/bin/spark-submit csb.jar synth kro -m all 15 $DATA/dataset_01/seed.mtx
```