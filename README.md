# Big Data Benchmarking Suite for Cyber-Security Analytics

The first publicly-available Big Data benchmarking suite for next-generation Intrusion Detection Systems, based on Property-Graphs.

## Motivation

A common trend in Intrusion Detection Systems (IDSs) is to consider data structures based on graphs to analyze network traffic and attack patterns. Timely detecting a threat is fundamental to reduce the risk to which the system is exposed, but no current studies aim at providing useful information to size Cloud or HPC infrastructures to meet certain service level objectives.

In this project we are researching, designing and implementing a distributed benchmark for the evaluation of the performance of next-generation IDSs.

Several studies employing big data benchmarks have been conducted over the years to evaluate and characterize various big data systems and architectures. However, most of the state-of-the-art big data benchmarks are designed for specific types of systems, and lack diversity of data and workloads. Moreover, the diversity and rapid evolution of big data systems impose challenges on workload selection and implementation, as it is unpractical to implement all big data workloads

Furthermore, the fidelity of the performance results in context of real applications, such as in the area of Cyber-Security, mandates the use of application-specific benchmarks that require application-specific data generators which synthetically scales up and down a synthetic data set and keeps this dataset similar to the real data. Synthetic graphs have advantage over the real graphs in terms of feasibility of finding a set of them covering a rich configuration space. However, very less approaches pays attention to keeping veracity of the real life data during the data generation.

For the above reasons, we offer a comprehensive suite which provides:
* Fast and flexible synthetic data generators with high degree veracity
* Intrusion detection representative workloads
* A user-friendly interface to monitor the cluster performance, showing application and system metrics

## Architecture

The suite is composed of three main components:
1. A dataset generator
2. Representative workloads
3. Metrics of interest and visualization

The first and second components are provided by the [CSB](csb/) module, while the third component is provided by the [Metrics Collector](metrics-collector/) module.

In addition, some example [datasets](data/) are provided.

## Licensing

This project is an open source product and it is supported under the GPLv3 license. For more information, see [LICENSE.txt](LICENSE.txt).
