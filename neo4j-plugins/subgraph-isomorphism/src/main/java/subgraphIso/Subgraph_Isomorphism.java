package subgraphIso;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.register.Register;


/**
 * Subgraph Isomorphism
 * A Java Plugin for Neo4j
 *
 *
 */
public class Subgraph_Isomorphism
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


    @Procedure("subgraphIso")
    @Description("Execute lucene query in the given index, return found nodes")
    public Stream<result> subgraphIso( @Name("query") String query, @Name("target") String target, @Name("parallelFactor") String parallelFactor,@Name("suppressResult") String suppressResult)
    {

        Label queryLabel=Label.label(query);//query label

        Label targetLabel=Label.label(target);//target label

        int pFactor=Integer.parseInt(parallelFactor);//parallel factor

        int numCores = Runtime.getRuntime().availableProcessors();// the available number of CPU cores

        ForkJoinPool threadPool=new ForkJoinPool(numCores*pFactor);

        boolean isSuppressed;

        if(suppressResult.equals("False")||suppressResult.equals("false")||suppressResult.equals("f")||suppressResult.equals("F"))

            isSuppressed=false;

        else

            isSuppressed=true;//by default, the results are suppressed. only execution time and total number of subgraphs are returned

        ArrayList<Node> queryNodeList=new ArrayList<>();//the list that store all query nodes

        long start=System.currentTimeMillis();

        List<List<Node>> matchedSubgraphs = UllmannAlg(queryLabel, targetLabel,queryNodeList,threadPool);

        long end=System.currentTimeMillis();

        ArrayList<result> resultList=new ArrayList<>();

        try {
            //return the results
            //each row of the matchedSubgraphs contains nodes in a matched subgraph ordered by the query nodes in the queryNodeList
            //i.e., queryNodeList.size()==matchedSubgraphs.get(i).size();

            if (matchedSubgraphs.isEmpty())
                return null;
            else {
                    if(!isSuppressed) {
                        for (int i = 0; i < matchedSubgraphs.size(); i++) {
                            for (int j = 0; j < matchedSubgraphs.get(i).size(); j++) {
                                resultList.add(new result(matchedSubgraphs.get(i).get(j),
                                        queryNodeList.get(j),
                                        Integer.toString(i),
                                        Integer.toString(matchedSubgraphs.size()),
                                        new String(Long.toString(end - start) + "ms")
                                ));
                            }
                        }

                        //Note: the result objects can only have String or Node type instance variables.
                        return resultList.stream();
                    }
                    else
                    {
                        resultList.add(new result(null,null,null, Integer.toString(matchedSubgraphs.size()),new String(Long.toString(end - start) + "ms")));
                        return resultList.stream();
                    }
                }
            } catch (Exception e) {
                String errMsg = "Error encountered while calculating subgraph isomorphism";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);}
   }





    private List<List<Node>> UllmannAlg(Label queryLabel, Label targetLabel,ArrayList<Node> queryNodeList, ForkJoinPool threadPool){


        List<List<Node>> queryNeighborList=new ArrayList<>();// the neighbor list for query vertices
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
            if (targetLabel.name().equals("All"))
                targetNodes = db.getAllNodes().iterator();
            else
                targetNodes = db.findNodes(targetLabel);

            int index=0;
            while (targetNodes.hasNext()) {
                Node targetNode = targetNodes.next();

                //pass the query graph nodes if all neo4j database is selected
                if (targetLabel.name().equals("All") && targetNode.hasLabel(queryLabel))
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
            List<List<Node>> candidateList = findCandidates(queryLabel, candidateListMap, nodeNeighborList, nodeNeighborListIndex,queryNodeList);


            //////////////////////////////////////////////////////////////////
            //Step 3: create the query graph's neighbor list
            queryNodeList.stream().forEach(node -> queryNeighborList.add(new ArrayList<>()));//insert empty lists
            queryNodeList.parallelStream().forEach(node ->
                    {
                        try(Transaction tx1= db.beginTx()) {
                            queryNeighborList.set(candidateListMap.get(node), findNodeNeighbors(node, queryLabel));
                            tx1.success();
                        }
                    });


            ////////////////////////////////////////////////////////
            //refine the candidate list preliminarily, check if it is valid
            refineCandidate(candidateList, queryNeighborList, nodeNeighborList, candidateListMap, nodeNeighborListMap);
            if (!isCorrect(candidateList)) {
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

            SubgraphProcessor mainProcessor=new SubgraphProcessor(candidateList,candidateListMap,candidateListSize,
                    queryNeighborList,
                    nodeNeighborList,nodeNeighborListMap,
                    threadPool);

            threadPool.execute(mainProcessor);

            while ((!mainProcessor.isDone()));

            threadPool.shutdown();

            matchedSubgraphs=mainProcessor.join();

            tx.success();

            //check redundant subgraphs
            if(!matchedSubgraphs.isEmpty())
            {

                for(int i=0;i<matchedSubgraphs.size();i++)
                {

                    for(int j=0;j<matchedSubgraphs.size();j++)
                    {
                        List<Node> subgraph =matchedSubgraphs.get(i);

                        if(i!=j)
                        {

                            Set<Node> subgraphSet=subgraph.stream().collect(Collectors.toSet());

                            Set<Node> verifySet=matchedSubgraphs.get(j).stream().collect(Collectors.toSet());

                            if(subgraphSet.equals(verifySet))matchedSubgraphs.remove(j);

                        }
                    }
                }

            }
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


    private void refineCandidate(List<List<Node>> candidateList,List<List<Node>> queryNeighborList,List<List<Node>> nodeNeighborList,
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
            boolean refinable = queryNeighborList.get(ii).parallelStream().allMatch(qnode ->
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


    private List<List<Node>> findCandidates(Label queryLabel, Map<Node,Integer> candidateListMap,List<List<Node>> nodeNeighborList, ArrayList<Node> nodeNeighborListIndex,ArrayList<Node> queryNodeList)
    {//find all query vertex candidates in the Neo4j database under the label "targetLabel"
        //input an empty candidate-list index list for modification
        List<List<Node>> candidateList = new ArrayList<>();
        try(Transaction tx= db.beginTx()) {
            ResourceIterator<Node> queryNodes = db.findNodes(queryLabel);

            while (queryNodes.hasNext()) {
                ArrayList<Node> candidateListPerVertex = new ArrayList<>();
                Node queryNode = queryNodes.next();
                int queryNodeDegree = findNodeNeighbors(queryNode, queryLabel).size();

                for (int i = 0; i < nodeNeighborList.size(); i++) {
                    int targetNodeDegree = nodeNeighborList.get(i).size();
                    if (targetNodeDegree >= queryNodeDegree)
                        candidateListPerVertex.add(nodeNeighborListIndex.get(i));
                }
                candidateListPerVertex.add(0, queryNode);// temporarily append the query node to the first position of the list
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
                queryNodeList.add(candidateList.get(i).get(0));
                candidateListMap.put(candidateList.get(i).get(0),i);
            }

            candidateList.stream().forEach(nodes -> nodes.remove(0));//remove the temporary query node at position 0
            queryNodes.close();
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
            if (targetLabel.name().equals("All"))
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