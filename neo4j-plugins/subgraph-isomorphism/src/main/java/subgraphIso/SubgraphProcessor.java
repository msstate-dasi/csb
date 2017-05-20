package subgraphIso;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.io.*;

import java.io.IOException;

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Fork;
import org.neo4j.graphdb.*;

public class SubgraphProcessor extends RecursiveTask<List<List<Node>>>{
    private List<List<Node>> candidateList;
    final private Map<Node,Integer> candidateNode2Index;
    final private int[] candidateListSize;
    final private List<List<Node>> queryNeighborList;
    final private List<List<Node>> nodeNeighborList;
    final private Map<Node,Integer> nodeNeighborListMap;
    final private int lo;
    final private int hi;
    final private ForkJoinPool pool;
    static final int THRESHOLD=1000;

    public SubgraphProcessor(List<List<Node>> candidateList, Map<Node,Integer> candidateNode2Index, int[] candidateListSize,
                             List<List<Node>> queryNeighborList,
                             List<List<Node>> nodeNeighborList, Map<Node,Integer> nodeNeighborListMap, ForkJoinPool pool)
    {
        this.lo=0;
        this.hi=candidateList.get(0).size();
        this.candidateList=candidateList;
        this.candidateNode2Index=candidateNode2Index;
        this.candidateListSize=candidateListSize;
        this.queryNeighborList=queryNeighborList;
        this.nodeNeighborList=nodeNeighborList;
        this.nodeNeighborListMap=nodeNeighborListMap;
        this.pool=pool;

    }

    @Override
    protected List<List<Node>> compute()
    {
        List<List<Node>> resultSubgraphs=new ArrayList<>();
        List<SubgraphProcessor> tasks=new ArrayList<>();

        //create a buff CandidateList for thread-safe modification
        List<List<Node>> buffCandidateList=new ArrayList<>(candidateList);
        for(int i=0;i<buffCandidateList.size();i++)
        {
            if(i==0)
                buffCandidateList.set(i,new ArrayList<>(candidateList.get(0).subList(lo,hi)));
            else
                buffCandidateList.set(i,new ArrayList<>(candidateList.get(i)));
        }

        if(hi-lo<THRESHOLD)
            resultSubgraphs.addAll(backtracking(0,buffCandidateList,candidateNode2Index,candidateListSize,queryNeighborList,nodeNeighborList,nodeNeighborListMap));
        else
        {
            int mid=(lo+hi)>>>1;
            buffCandidateList.set(0,new ArrayList<>(candidateList.get(0).subList(lo,mid)));
            SubgraphProcessor forkedTask1=new SubgraphProcessor(buffCandidateList,candidateNode2Index,candidateListSize,queryNeighborList,nodeNeighborList,nodeNeighborListMap,pool);
            buffCandidateList.set(0,new ArrayList<>(candidateList.get(0).subList(mid,hi)));
            SubgraphProcessor forkedTask2=new SubgraphProcessor(buffCandidateList,candidateNode2Index,candidateListSize,queryNeighborList,nodeNeighborList,nodeNeighborListMap,pool);
//            pool.execute(forkedTask1);
//            pool.execute(forkedTask2);
            forkedTask1.fork();
            forkedTask2.fork();
            tasks.add(forkedTask1);
            tasks.add(forkedTask2);
        }

        addResultsFromTasks(resultSubgraphs,tasks);
        return resultSubgraphs;



    }

    private List<List<Node>> backtracking(int numLayer, List<List<Node>> candidateList,Map<Node,Integer> candidateNode2Index,int[] candidateListSize,
                                          List<List<Node>> queryNeighborList,
                                          List<List<Node>> nodeNeighborList, Map<Node,Integer> nodeNeighborListMap)
    {

        List<List<Node>> matchedSubgraphs=new ArrayList<>();

        if(numLayer==queryNeighborList.size())
        {


            ArrayList<Node> subgraph=new ArrayList<>();

            for(int i=0;i<candidateList.size();i++)
            {
                // System.out.println(candidateList.get(i));
                subgraph.add(candidateList.get(i).get(0));
            }
            matchedSubgraphs.add(subgraph);
            return matchedSubgraphs;
        }

        List<List<Node>> originalCandidateList= copyNodeList(candidateList);
        //retain the original copy of the candidateList for rolling back
        for(int i=0;i<candidateList.get(numLayer).size();i++)
        {
            if(numLayer==0) System.out.println("Working on Iteration "+i);
            ArrayList<Node> singleNode=new ArrayList<>();
            singleNode.add(candidateList.get(numLayer).get(i));

            candidateList.get(numLayer).retainAll(singleNode);//select the single node in that row

            removeUniqueNodes(singleNode.get(0),candidateList,candidateListSize,numLayer,false);//remove unique nodes in other rows
            refineCandidate(candidateList,queryNeighborList,nodeNeighborList,candidateNode2Index,nodeNeighborListMap);//refine the candidate list
            if(isCorrect(candidateList)) {
                List<List<Node>> pendingResult=backtracking(numLayer + 1, candidateList, candidateNode2Index, candidateListSize, queryNeighborList, nodeNeighborList, nodeNeighborListMap);
                if(!pendingResult.isEmpty())
                    matchedSubgraphs.addAll(pendingResult);
            }

            removeUniqueNodes(originalCandidateList.get(numLayer).get(i), originalCandidateList, candidateListSize,numLayer+1,true);//trim the branches


            candidateList=copyNodeList(originalCandidateList);//resume the candidate list and continue searching

//            if(numLayer==0) System.out.println("Found "+subgraphCount+" subgraphs at Iteration "+i);
        }
        return matchedSubgraphs;
    }

    private void addResultsFromTasks(List<List<Node>> list, List<SubgraphProcessor> tasks)

    {
        for (SubgraphProcessor item:tasks)
            list.addAll(item.join());
    }

    private List<List<Node>> copyNodeList(List<List<Node>> originalList)
    {
        List<List<Node>> copyList=new ArrayList<>();
        for(int i=0;i<originalList.size();i++)
        {
            ArrayList<Node> temp=new ArrayList<>();
            copyList.add(temp);
            for(int j=0;j<originalList.get(i).size();j++)
            {
                copyList.get(i).add(originalList.get(i).get(j));

            }
        }
        return copyList;

    }

    private void refineCandidate(List<List<Node>> candidateList,List<List<Node>> queryNeighborList,List<List<Node>> nodeNeighborList,
                                 Map<Node,Integer> candidateNode2Index,Map<Node,Integer> nodeNeighborListMap) {//given the three lists, refine the candidate list


        long start = System.currentTimeMillis();

        //   if(candidateList.stream().filter(list->list.size()>0).collect(toList()).isEmpty())return;


        List<List<Node>> nodesToRemove = new ArrayList<>();

        candidateList.stream().forEach(list -> nodesToRemove.add(new ArrayList<>()));//create the list of node that should be removed


        IntStream.range(0, candidateList.size()).parallel().forEach(ii -> candidateList.get(ii).stream().forEach(node -> {
            boolean refinable = queryNeighborList.get(ii).parallelStream().allMatch(qnode ->
                    candidateList.get(candidateNode2Index.get(qnode)).parallelStream().anyMatch(subnode ->
                            nodeNeighborList.get(nodeNeighborListMap.get(node)).contains(subnode)));

            if (!refinable) {

                nodesToRemove.get(ii).add(node);

            }
        }));


        //Now remove the nodes from the candidate list

        for (int i = 0; i < candidateList.size(); i++) {
            candidateList.get(i).removeAll(nodesToRemove.get(i));
        }

    }
    private boolean isCorrect(List<List<Node>> candidateList)
    {//check if the current candidate list is correct. (i.e., is there any empty candidate list?)
        for(int i=0;i<candidateList.size();i++)
        {
            if(candidateList.get(i).isEmpty()) return false;

        }
        return true;

    }

    private void removeUniqueNodes(Node unique, List<List<Node>> candidateList,int[] candidateListSize, int layer,boolean TrimBranch)
    {

        if(!TrimBranch) {
            for (int i = layer+1; i < candidateList.size(); i++) {
                if (candidateList.get(i).contains(unique) ) {
                    candidateList.get(i).remove(unique);
                }
            }
        }
        else{
            //for trimming, pick nodes that have the same candidate list size as the node in the current layer
            int currentLayer=layer-1;
            for(int i=layer;i<candidateList.size();i++) {
                if (candidateListSize[i] == candidateListSize[currentLayer])
                {
                    if(candidateList.get(i).contains(unique) )
                        candidateList.get(i).remove(unique);
                }
                else
                    break;// the main candidate list is sorted, so we break here when different size is found
            }

        }
    }



}
