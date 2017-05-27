# subgraph-isomorphism-neo4j

This project under @msstate-dasi provides a subgraph isomorphism Java plugin for Neo4j database. Given a pattern graph and a target graph, it calculates all possible subgraphs of the target graph isomorphic to the pattern graph. Both the pattern graph and target graph are stored in the same Neo4j database. Currently it utilized the Ullmann's algorithm and works only with undirected graphs (ignore directions in a directional graph).

## Compile: 

`mvn compile`

## Install:

`mvn clean package`

Copy the JAR file `subgraph-isomorphism-0.x-SNAPSHOT.jar` under `target\` to your Neo4j plugin folder and restart the Neo4j server.

## Usage

This plugin is supposed to be used in the Cypher console, either in the Web browser or in the commandline console.

`CALL csb.subgraphIsomorphism(patternLabel, targetLabel) YIELD subgraphIndex, patternNode, targetNode`

`CALL csb.subgraphIsomorphism(patternLabel, targetLabel, parallelFactor) YIELD subgraphIndex, patternNode, targetNode`

### Parameters:

patternLabel (String): the Neo4j database label of the pattern graph

targetLabel (String): the Neo4j database label of the target graph. If the empty string `""` is used, all nodes except nodes with patternLabel in the database will be analyzed.

parallelFactor (Long): define the size of the thread pool. The number of threads in the pool is parallelFactor*NumberofCores.


### Return:
All matching subgraphs and related info. Three columns:

1. Isomorphic subgraph index 

2. Pattern graph nodes

2. Isomorphic subgraph nodes in the target graph
