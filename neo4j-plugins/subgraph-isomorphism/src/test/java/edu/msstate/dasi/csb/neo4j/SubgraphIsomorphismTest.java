package edu.msstate.dasi.csb.neo4j;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test for the subgraph isomorphism Java plugin
 */
public class SubgraphIsomorphismTest {

    //     This rule starts a Neo4j instance
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the procedure we want to test
            .withProcedure(SubgraphIsomorphism.class);

    //    This is the 1/3 tests for the subgraph isomorphism java plugin
    @Test
    public void test1() throws Throwable {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withoutEncryption().toConfig())) {
            // create a session for the test
            Session session = driver.session();

            //create the first pattern graph for the test, it is also the target graph
            session.run("CREATE (p:Pattern1 {number:'1'})");

            session.run("CREATE (p:Pattern1 {number:'2'})");

            session.run("CREATE (p:Pattern1 {number:'3'})");

            session.run("CREATE (p:Pattern1 {number:'4'})");

            session.run("CREATE (p:Pattern1 {number:'5'})");

            session.run("MATCH (a:Pattern1),(b:Pattern1) WHERE a.number='1' AND b.number='5' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern1),(b:Pattern1) WHERE a.number='2' AND b.number='5' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern1),(b:Pattern1) WHERE a.number='3' AND b.number='5' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern1),(b:Pattern1) WHERE a.number='4' AND b.number='5' CREATE (a)-[r:connected]->(b)");


            //execute the plugin as a Cypher query and fetch the result
            StatementResult result = session.run("CALL csb.subgraphIsomorphism('Pattern1','Pattern1')");

            List<Record> resultList = result.list();//convert the result record stream to list

            assertThat(resultList.size(), is(120)); //the result has correct number of subgraphs

            assertThat(result.keys().size(), is(3));//the result has correct number of keys

            assertThat(result.keys(), contains("subgraphIndex", "patternNode", "targetNode"));//the result has correct ordered keys

            //split the actual result record into columns
            List<Long> actualColumn1 = new ArrayList<>();

            List<String> actualColumn2 = new ArrayList<>();

            List<String> actualColumn3 = new ArrayList<>();

            // fetch each column of the result record
            resultList.forEach(record -> {

                actualColumn1.add(record.get(0).asLong());

                actualColumn2.add(record.get(1).toString());

                actualColumn3.add(record.get(2).toString());

            });

            //construct the expected result columns
            List<Long> expectedColumn1 = new ArrayList<>();

            List<String> expectedColumn2 = new ArrayList<>();

            List<String> expectedColumn3 = new ArrayList<>(Arrays.asList(
                    "node<0>", "node<1>", "node<2>", "node<3>", "node<4>",
                    "node<0>", "node<1>", "node<3>", "node<2>", "node<4>",
                    "node<0>", "node<2>", "node<1>", "node<3>", "node<4>",
                    "node<0>", "node<2>", "node<3>", "node<1>", "node<4>",
                    "node<0>", "node<3>", "node<1>", "node<2>", "node<4>",
                    "node<0>", "node<3>", "node<2>", "node<1>", "node<4>",
                    "node<1>", "node<0>", "node<2>", "node<3>", "node<4>",
                    "node<1>", "node<0>", "node<3>", "node<2>", "node<4>",
                    "node<1>", "node<2>", "node<0>", "node<3>", "node<4>",
                    "node<1>", "node<2>", "node<3>", "node<0>", "node<4>",
                    "node<1>", "node<3>", "node<0>", "node<2>", "node<4>",
                    "node<1>", "node<3>", "node<2>", "node<0>", "node<4>",
                    "node<2>", "node<0>", "node<1>", "node<3>", "node<4>",
                    "node<2>", "node<0>", "node<3>", "node<1>", "node<4>",
                    "node<2>", "node<1>", "node<0>", "node<3>", "node<4>",
                    "node<2>", "node<1>", "node<3>", "node<0>", "node<4>",
                    "node<2>", "node<3>", "node<0>", "node<1>", "node<4>",
                    "node<2>", "node<3>", "node<1>", "node<0>", "node<4>",
                    "node<3>", "node<0>", "node<1>", "node<2>", "node<4>",
                    "node<3>", "node<0>", "node<2>", "node<1>", "node<4>",
                    "node<3>", "node<1>", "node<0>", "node<2>", "node<4>",
                    "node<3>", "node<1>", "node<2>", "node<0>", "node<4>",
                    "node<3>", "node<2>", "node<0>", "node<1>", "node<4>",
                    "node<3>", "node<2>", "node<1>", "node<0>", "node<4>"));

            for (Long i = 0L; i < 24; i++) {

                for (int j = 0; j < 5; j++)
                    expectedColumn1.add(i);

                expectedColumn2.addAll(Arrays.asList("node<3>", "node<0>", "node<1>", "node<2>", "node<4>"));

            }


            //check the actual result
            assertThat(actualColumn1, equalTo(expectedColumn1));

            assertThat(actualColumn2, equalTo(expectedColumn2));

            assertThat(actualColumn3, equalTo(expectedColumn3));

            session.close();

            driver.close();
        }
    }

    //    This is the 2/3 tests
    @Test
    public void test2() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withoutEncryption().toConfig())) {

            Session session = driver.session();

            //create the second pattern graph
            session.run("CREATE (p:Pattern2 {number:'1'})");

            session.run("CREATE (p:Pattern2 {number:'2'})");

            session.run("CREATE (p:Pattern2 {number:'3'})");

            session.run("MATCH (a:Pattern2),(b:Pattern2) WHERE a.number='1' AND b.number='2' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern2),(b:Pattern2) WHERE a.number='1' AND b.number='3' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern2),(b:Pattern2) WHERE a.number='2' AND b.number='3' CREATE (a)-[r:connected]->(b)");

            //create the second target graph
            session.run("CREATE (p:Target2 {number:'4'})");

            session.run("CREATE (p:Target2 {number:'5'})");

            session.run("CREATE (p:Target2 {number:'6'})");

            session.run("CREATE (p:Target2 {number:'7'})");

            session.run("MATCH (a:Target2),(b:Target2) WHERE a.number='4' AND b.number='5' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target2),(b:Target2) WHERE a.number='4' AND b.number='6' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target2),(b:Target2) WHERE a.number='4' AND b.number='7' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target2),(b:Target2) WHERE a.number='5' AND b.number='6' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target2),(b:Target2) WHERE a.number='6' AND b.number='7' CREATE (a)-[r:connected]->(b)");

            //execute the plugin as a Cypher query and fetch the result
            StatementResult result = session.run("CALL csb.subgraphIsomorphism('Pattern2','Target2')");

            List<Record> resultList = result.list();//convert the result record stream to list

            assertThat(resultList.size(), is(6)); //the result has correct number of subgraphs

            assertThat(result.keys().size(), is(3));//the result has correct number of keys

            assertThat(result.keys(), contains("subgraphIndex", "patternNode", "targetNode"));//the result has correct ordered keys

            //split the actual result record into columns
            List<Long> actualColumn1 = new ArrayList<>();

            List<String> actualColumn2 = new ArrayList<>();

            List<String> actualColumn3 = new ArrayList<>();

            // fetch each column of the result record
            resultList.forEach(record -> {

                actualColumn1.add(record.get(0).asLong());

                actualColumn2.add(record.get(1).toString());

                actualColumn3.add(record.get(2).toString());

            });

            //construct the expected result columns
            List<Long> expectedColumn1 = new ArrayList<>();

            List<String> expectedColumn2 = new ArrayList<>();

            List<String> expectedColumn3 = new ArrayList<>(Arrays.asList(
                    "node<5>", "node<4>", "node<3>",
                    "node<6>", "node<5>", "node<3>"));

            for (Long i = 0L; i < 2; i++) {

                for (int j = 0; j < 3; j++)
                    expectedColumn1.add(i);

                expectedColumn2.addAll(Arrays.asList("node<2>", "node<1>", "node<0>"));

            }


            //check the actual result
            assertThat(actualColumn1, equalTo(expectedColumn1));

            assertThat(actualColumn2, equalTo(expectedColumn2));

            assertThat(actualColumn3, equalTo(expectedColumn3));

            session.close();

            driver.close();
        }

    }

    //    This is the 3/3 tests
    @Test
    public void test3() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withoutEncryption().toConfig())) {

            Session session = driver.session();

            //create the third pattern graph
            session.run("CREATE (p:Pattern3 {number:'1'})");

            session.run("CREATE (p:Pattern3 {number:'2'})");

            session.run("CREATE (p:Pattern3 {number:'3'})");

            session.run("CREATE (p:Pattern3 {number:'4'})");

            session.run("MATCH (a:Pattern3),(b:Pattern3) WHERE a.number='1' AND b.number='2' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern3),(b:Pattern3) WHERE a.number='1' AND b.number='3' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern3),(b:Pattern3) WHERE a.number='2' AND b.number='3' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Pattern3),(b:Pattern3) WHERE a.number='3' AND b.number='4' CREATE (a)-[r:connected]->(b)");

            //create the third target graph
            session.run("CREATE (p:Target3 {number:'5'})");

            session.run("CREATE (p:Target3 {number:'6'})");

            session.run("CREATE (p:Target3 {number:'7'})");

            session.run("CREATE (p:Target3 {number:'8'})");

            session.run("CREATE (p:Target3 {number:'9'})");

            session.run("CREATE (p:Target3 {number:'10'})");

            session.run("MATCH (a:Target3),(b:Target3) WHERE a.number='5' AND b.number='6' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target3),(b:Target3) WHERE a.number='5' AND b.number='8' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target3),(b:Target3) WHERE a.number='7' AND b.number='8' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target3),(b:Target3) WHERE a.number='8' AND b.number='9' CREATE (a)-[r:connected]->(b)");

            session.run("MATCH (a:Target3),(b:Target3) WHERE a.number='9' AND b.number='10' CREATE (a)-[r:connected]->(b)");

            //execute the plugin as a Cypher query and fetch the result
            StatementResult result = session.run("CALL csb.subgraphIsomorphism('Pattern3','Target3')");

            List<Record> resultList = result.list();//convert the result record stream to list

            assertThat(resultList.size(), is(0)); //the result should be empty: no matching subgraphs

            assertThat(result.keys().size(), is(3));//the result has correct number of keys

            assertThat(result.keys(), contains("subgraphIndex", "patternNode", "targetNode"));//the result has correct ordered keys

            session.close();

            driver.close();

        }

    }

}