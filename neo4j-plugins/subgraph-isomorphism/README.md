# subgraph-isomorphism-neo4j

This project under @msstate-dasi provides a subgraph isomorphism Java plugin for Neo4j database. Given a query graph and a target graph, it calculates all possible subgraphs of the target graph isomorphic to the query graph. Both the query graph and target graph are stored in the same Neo4j database. Currently it utilized the Ullmann's algorithm and works only with undirected graphs (ignore directions in a directional graph).

## Compile: 

`mvn compile`

## Install:

`mvn clean package`

Copy the JAR file `subgraph-isomorphism-0.x-SNAPSHOT.jar` under `target\` to your Neo4j plugin folder and restart the Neo4j server.

## Usage

This plugin is supposed to be used in the Cypher console, either in the Web browser or in the commandline console.

`call SubgraphIso("QueryLabel","TargetLabel")`

### Arguments:

QueryLabel: the Neo4j database label of the query graph

TargetLabel: the Neo4j database label of the target graph. if use "All", all nodes in the dataset will be calculated

### Return:
All matching subgraphs and related info. Four columns:

1. Isomorphic subgraph nodes

2. Matching query graph nodes

3. Subgraph index: which subgraph does the resultNode belong to

4. Total number of subgraphs
