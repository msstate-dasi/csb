package edu.msstate.dasi.csb.neo4j;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;


/**
 * Subgraph Isomorphism
 * A Java Plugin for Neo4j
 *
 *
 */
public class SubgraphIsomorphism
{
    // Only static fields and @Context-annotated fields are allowed in
    // Procedure classes.


    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseAPI db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;


    @Procedure("csb.subgraphIsomorphism")
    @Description("Given the pattern graph label and the target graph label, execute subgraph isomorphism algorithm and return subgraphs. " +
            "CALL csb.subgraphIsomorphism(patternLabel, targetLabel) YIELD subgraphIndex, patternNode, targetNode")
    public Stream<Result> subgraphIsomorphism(@Name("patternLabel") String patternLabelString, @Name("targetLabel") String targetLabelString,
                                      @Name(value = "parallelFactor",defaultValue = "2") Long parallelFactor)
    {

        Label patternLabel=Label.label(patternLabelString);//pattern label

        Label targetLabel=Label.label(targetLabelString);//target label

//        int parallelFactor=Integer.parseInt(parallelFactorString);//parallel factor

        int numCores = Runtime.getRuntime().availableProcessors();// the available number of CPU cores

        ForkJoinPool threadPool=new ForkJoinPool(numCores*parallelFactor.intValue());

        ArrayList<Node> patternNodeList=new ArrayList<>();//the list that store all pattern nodes

        List<List<Node>> matchedSubgraphs = ullmannAlg(patternLabel, targetLabel,patternNodeList,threadPool);//execute the algorithm

        ArrayList<Result> resultList=new ArrayList<>();

        try {
            //return the results
            //each row of the matchedSubgraphs contains nodes in a matched subgraph ordered by the pattern nodes in the patternNodeList
            //i.e., patternNodeList.size()==matchedSubgraphs.get(i).size();

            if (matchedSubgraphs.isEmpty())

                return null;

            else {

                        for (Long i = 0L; i < matchedSubgraphs.size(); i++)
                        {

                            for (int j = 0; j < matchedSubgraphs.get(i.intValue()).size(); j++)
                            {

                                resultList.add(new Result(

                                        i,

                                        patternNodeList.get(j),

                                        matchedSubgraphs.get(i.intValue()).get(j)

                                       ));
                            }
                        }


                        return resultList.stream();
                }

            } catch (Exception e) {

                String errMsg = new String("Error encountered while calculating subgraph isomorphism.");

                log.error(errMsg, e);

                throw new RuntimeException(errMsg, e);}
   }





    private List<List<Node>> ullmannAlg(Label patternLabel, Label targetLabel,ArrayList<Node> patternNodeList, ForkJoinPool threadPool){


        List<List<Node>> patternNeighborList=new ArrayList<>();// the neighbor list for pattern vertices
        List<List<Node>> nodeNeighborList=new ArrayList<>();// the neighbor list for the Neo4j database with the target label
        List<List<Node>> matchedSubgraphs=new ArrayList<>();//store the final results

        try(Transaction tx=db.beginTx()) {

            /**
             * Note: the sequence of the following three steps is strict.
             **/

            //Step 1: create the neighbor list for the Neo4j database with the target label
            ResourceIterator<Node> targetNodes;
            ArrayList<Node> nodeNeighborListIndex = new ArrayList<>();
            Map<Node,Integer> nodeNeighborListMap=new HashMap<>();//given an node, get the index in the node neighbor list
            if (targetLabel.name().equals(""))
                targetNodes = db.getAllNodes().iterator();
            else
                targetNodes = db.findNodes(targetLabel);

            int index=0;
            while (targetNodes.hasNext()) {
                Node targetNode = targetNodes.next();

                //pass the pattern graph nodes if all neo4j database is selected
                if (targetLabel.name().equals("") && targetNode.hasLabel(patternLabel))
                    continue;

                nodeNeighborList.add(findNodeNeighbors(targetNode, targetLabel));
                nodeNeighborListIndex.add(targetNode);
                nodeNeighborListMap.put(targetNode,index);
                index++;
            }

            targetNodes.close();


           ///////////////////////////////////////////////////////////////
            //Step 2: create the candidate list and its index
            Map<Node,Integer> candidateListMap=new HashMap<>();
            List<List<Node>> candidateList = findCandidates(patternLabel, candidateListMap, nodeNeighborList, nodeNeighborListIndex,patternNodeList);


            //////////////////////////////////////////////////////////////////
            //Step 3: create the pattern graph's neighbor list
            patternNodeList.stream().forEach(node -> patternNeighborList.add(new ArrayList<>()));//insert empty lists
            patternNodeList.parallelStream().forEach(node ->
                    {
                        try(Transaction tx1= db.beginTx()) {
                            patternNeighborList.set(candidateListMap.get(node), findNodeNeighbors(node, patternLabel));
                            tx1.success();
                        }
                    });


            ////////////////////////////////////////////////////////
            //refine the candidate list preliminarily, check if it is valid
            refineCandidate(candidateList, patternNeighborList, nodeNeighborList, candidateListMap, nodeNeighborListMap);
            if (!isCorrect(candidateList)) {
                tx.success();
                return matchedSubgraphs;
            }

            ////////////////////////////////////////////////////////////////
            //store the size of each element in the candidate list in an array
            int[] candidateListSize = new int[candidateList.size()];

            for (int i = 0; i < candidateList.size(); i++) {
                candidateListSize[i] = candidateList.get(i).size();
            }

            //////////////////////////////////////////////////////////////
            //begin multithreading execution of the algorithm


            int threadPoolSize=threadPool.getParallelism();
            int jobSize=candidateList.get(0).size();
            final long splitSize = (jobSize>threadPoolSize*2)?jobSize/(threadPoolSize*2):1;
            SubgraphProcessor mainProcessor=new SubgraphProcessor(candidateList,candidateListMap,candidateListSize,
                    patternNeighborList,
                    nodeNeighborList,nodeNeighborListMap,splitSize,
                    threadPool);

            Future<List<List<Node>>> futureMatchedSubgraphs=threadPool.submit(mainProcessor);

            try{ matchedSubgraphs.addAll(futureMatchedSubgraphs.get());}

            catch (Exception e)
            {
                e.printStackTrace();
            }

            threadPool.shutdown();

            tx.success();

        }
        return matchedSubgraphs;
    }


    private boolean isCorrect(List<List<Node>> candidateList)
    {//check if the current candidate list is correct. (i.e., is there any empty candidate list?)
        for(int i=0;i<candidateList.size();i++)
        {
            if(candidateList.get(i).isEmpty()) return false;

        }
        return true;
    }


    private void refineCandidate(List<List<Node>> candidateList,List<List<Node>> patternNeighborList,List<List<Node>> nodeNeighborList,
                                 Map<Node,Integer> candidateListMap, Map<Node,Integer> nodeNeighborListMap)
    {//given the three lists, refine the candidate list


        //check if there are empty entries in the candidate list

        for(List<Node> list: candidateList)
        {
            if(list.size()==0)return;

        }

        List<List<Node>> nodesToRemove=new ArrayList<>();
        candidateList.stream().forEach(list->nodesToRemove.add(new ArrayList<>()));//create the list of node that should be removed

        IntStream.range(0,candidateList.size()).parallel().forEach(ii->candidateList.get(ii).stream().forEach(node-> {
            boolean refinable = patternNeighborList.get(ii).parallelStream().allMatch(qnode ->
                    candidateList.get(candidateListMap.get(qnode)).parallelStream().anyMatch(subnode ->
                            nodeNeighborList.get(nodeNeighborListMap.get(node)).contains(subnode)));

            if (!refinable) {

                nodesToRemove.get(ii).add(node);

            }
        }));
        //Now remove the nodes from the candidate list

        for (int i=0;i<candidateList.size();i++)
        {
            candidateList.get(i).removeAll(nodesToRemove.get(i));
        }

    }


    private List<List<Node>> findCandidates(Label patternLabel, Map<Node,Integer> candidateListMap,List<List<Node>> nodeNeighborList, ArrayList<Node> nodeNeighborListIndex,ArrayList<Node> patternNodeList)
    {//find all pattern vertex candidates in the Neo4j database under the label "targetLabel"
        //input an empty candidate-list index list for modification
        List<List<Node>> candidateList = new ArrayList<>();
        try(Transaction tx= db.beginTx()) {
            ResourceIterator<Node> patternNodes = db.findNodes(patternLabel);

            while (patternNodes.hasNext()) {
                ArrayList<Node> candidateListPerVertex = new ArrayList<>();
                Node patternNode = patternNodes.next();
                int patternNodeDegree = findNodeNeighbors(patternNode, patternLabel).size();

                for (int i = 0; i < nodeNeighborList.size(); i++) {
                    int targetNodeDegree = nodeNeighborList.get(i).size();
                    if (targetNodeDegree >= patternNodeDegree)
                        candidateListPerVertex.add(nodeNeighborListIndex.get(i));
                }
                candidateListPerVertex.add(0, patternNode);// temporarily append the pattern node to the first position of the list
                candidateList.add(candidateListPerVertex);
            }

            //sort the candidate list by the number of candidates (ascending)
            Comparator<List<Node>> candidateListSizeComparator = new Comparator<List<Node>>() {
                @Override
                public int compare(List<Node> o1, List<Node> o2) {

                    return o1.size() - o2.size();
                }
            };
            candidateList.sort(candidateListSizeComparator);

            for (int i=0;i<candidateList.size();i++)
            {
                patternNodeList.add(candidateList.get(i).get(0));
                candidateListMap.put(candidateList.get(i).get(0),i);
            }

            candidateList.stream().forEach(nodes -> nodes.remove(0));//remove the temporary pattern node at position 0
            patternNodes.close();
            tx.success();
        }
        return candidateList;
    }

    private ArrayList<Node> findNodeNeighbors(Node node, Label targetLabel)
    {//find the neighbors of a node in the Neo4j database
        ArrayList<Node> neighborsOfnode=new ArrayList<>();
        Iterator<Relationship> relationships=node.getRelationships().iterator();
        while(relationships.hasNext()){
            Node neighborNode=relationships.next().getOtherNode(node);
            if (targetLabel.name().equals(""))
            {
                neighborsOfnode.add(neighborNode);
            }
            else if (neighborNode.hasLabel(targetLabel))
            {
                neighborsOfnode.add(neighborNode);// only add nodes that have the target label and ignore others
            }

        }
        return neighborsOfnode;
    }


}
