package edu.msstate.dasi.csb.neo4j;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.IntStream;

public class SubgraphProcessor extends RecursiveTask<List<List<Node>>> {

    final private Map<Node, Integer> candidateNode2Index;
    final private List<List<Node>> patternInNeighborList;
    final private List<List<Node>> patternOutNeighborList;
    final private List<List<Node>> targetInNeighborList;
    final private List<List<Node>> targetOutNeighborList;
    final private Map<Node, Integer> nodeNeighborListMap;
    final private int lo;
    final private int hi;
    final private ForkJoinPool threadPool;
    final private long splitSize;
    private List<List<Node>> candidateList;
    private List<List<Node>> matchedSubgraphs;

    SubgraphProcessor(List<List<Node>> candidateList,
                      Map<Node, Integer> candidateNode2Index,
                      List<List<Node>> patternInNeighborList, List<List<Node>> patternOutNeighborList,
                      List<List<Node>> targetInNeighborList, List<List<Node>> targetOutNeighborList,
                      Map<Node, Integer> nodeNeighborListMap,
                      long splitSize,
                      ForkJoinPool threadPool) {

        this.lo = 0;
        this.hi = candidateList.get(0).size();
        this.candidateList = candidateList;
        this.candidateNode2Index = candidateNode2Index;
        this.patternInNeighborList = patternInNeighborList;
        this.patternOutNeighborList = patternOutNeighborList;
        this.targetInNeighborList = targetInNeighborList;
        this.targetOutNeighborList = targetOutNeighborList;
        this.nodeNeighborListMap = nodeNeighborListMap;
        this.threadPool = threadPool;
        this.matchedSubgraphs = new ArrayList<>();
        this.splitSize = splitSize;

    }

    @Override
    protected List<List<Node>> compute() {
        // Assign tasks to different threads
        List<SubgraphProcessor> tasks = new ArrayList<>();

        if (hi - lo <= splitSize) {
            // A task is small enough for a single thread
            matchedSubgraphs.addAll(backtracking(0, candidateList, candidateNode2Index,
                    patternInNeighborList, patternOutNeighborList,
                    targetInNeighborList, targetOutNeighborList, nodeNeighborListMap));
        } else {
            // A task is going to be split in half
            int mid = (lo + hi) >>> 1;

            List<List<Node>> leftCandidateList = copyNodeList(candidateList, lo, mid);

            List<List<Node>> rightCandidateList = copyNodeList(candidateList, mid, hi);

            SubgraphProcessor forkedTask1 = new SubgraphProcessor(leftCandidateList, candidateNode2Index,
                    patternInNeighborList, patternOutNeighborList,
                    targetInNeighborList, targetOutNeighborList, nodeNeighborListMap,
                    splitSize, threadPool);

            SubgraphProcessor forkedTask2 = new SubgraphProcessor(rightCandidateList, candidateNode2Index,
                    patternInNeighborList, patternOutNeighborList,
                    targetInNeighborList, targetOutNeighborList, nodeNeighborListMap,
                    splitSize, threadPool);

            // Don't use two fork() here, as that will make the current thread idle waiting for the forked threads until
            // they finish the task
            invokeAll(forkedTask1, forkedTask2);

            tasks.add(forkedTask1);

            tasks.add(forkedTask2);

            collectResultsFromTasks(matchedSubgraphs, tasks);
        }

        return matchedSubgraphs;
    }

    /**
     * Backtracking the candidate list to find matching subgraphs
     */
    private List<List<Node>> backtracking(int numLayer,
                                          List<List<Node>> candidateList,
                                          Map<Node, Integer> candidateNode2Index,
                                          List<List<Node>> patternInNeighborList, List<List<Node>> patternOutNeighborList,
                                          List<List<Node>> targetInNeighborList, List<List<Node>> targetOutNeighborList,
                                          Map<Node, Integer> nodeNeighborListMap) {

        List<List<Node>> matchedSubgraphs = new ArrayList<>();

        if (numLayer == candidateList.size()) {
            // A matching subgraph is found
            ArrayList<Node> subgraph = new ArrayList<>();

            for (List<Node> aCandidateList : candidateList) {
                subgraph.add(aCandidateList.get(0));
            }

            matchedSubgraphs.add(subgraph);

            return matchedSubgraphs;
        }

        List<List<Node>> originalCandidateList = copyNodeList(candidateList, 0, candidateList.get(0).size());
        // Retain the original copy of the candidateList for rolling back
        for (int i = 0; i < candidateList.get(numLayer).size(); i++) {

            ArrayList<Node> singleNode = new ArrayList<>();

            singleNode.add(candidateList.get(numLayer).get(i));

            // Select the single node in that row
            candidateList.get(numLayer).retainAll(singleNode);

            // Remove unique nodes in other rows
            removeUniqueNodes(singleNode.get(0), candidateList, numLayer);

            // Refine the candidate list
            refineCandidate(candidateList, patternInNeighborList, patternOutNeighborList, targetInNeighborList, targetOutNeighborList, candidateNode2Index, nodeNeighborListMap);

            if (isCorrect(candidateList)) {
                // If the candidate list is valid, go to the next round recursively
                List<List<Node>> pendingResult = backtracking(numLayer + 1, candidateList, candidateNode2Index,
                        patternInNeighborList, patternOutNeighborList, targetInNeighborList, targetOutNeighborList, nodeNeighborListMap);

                if (!pendingResult.isEmpty()) matchedSubgraphs.addAll(pendingResult);
            }

            // Resume the candidate list and continue searching
            candidateList = copyNodeList(originalCandidateList, 0, originalCandidateList.get(0).size());

        }
        return matchedSubgraphs;
    }

    /**
     * Collect matching subgraphs from forked tasks
     */
    private void collectResultsFromTasks(List<List<Node>> list, List<SubgraphProcessor> tasks) {

        for (SubgraphProcessor item : tasks) list.addAll(item.join());
    }

    private List<List<Node>> copyNodeList(List<List<Node>> originalList, int start, int end) {

        List<List<Node>> copyList = new ArrayList<>();

        ArrayList<Node> firstRow = new ArrayList<>();

        for (int k = start; k < end; k++) {
            firstRow.add(originalList.get(0).get(k));
        }

        copyList.add(firstRow);

        for (int i = 1; i < originalList.size(); i++) {
            ArrayList<Node> temp = new ArrayList<>();

            temp.addAll(originalList.get(i));

            copyList.add(temp);
        }

        return copyList;
    }

    /**
     * Given the candidate and neighbor lists, refine the candidate list
     */
    private void refineCandidate(List<List<Node>> candidateList,
                                 List<List<Node>> patternInNeighborList,
                                 List<List<Node>> patternOutNeighborList,
                                 List<List<Node>> targetInNeighborList,
                                 List<List<Node>> targetOutNeighborList,
                                 Map<Node, Integer> candidateListMap,
                                 Map<Node, Integer> nodeNeighborListMap) {

        List<List<Node>> nodesToRemove = new ArrayList<>();

        // Create the list of node that should be removed
        candidateList.forEach(list -> nodesToRemove.add(new ArrayList<>()));

        IntStream.range(0, candidateList.size()).parallel().forEach(ii -> candidateList.get(ii).forEach(node -> {

            boolean inRefinable = patternInNeighborList.get(ii).parallelStream().allMatch(qnode ->
                    candidateList.get(candidateListMap.get(qnode)).parallelStream().anyMatch(subnode ->
                            targetInNeighborList.get(nodeNeighborListMap.get(node)).contains(subnode)));

            boolean outRefinable = patternOutNeighborList.get(ii).parallelStream().allMatch(qnode ->
                    candidateList.get(candidateListMap.get(qnode)).parallelStream().anyMatch(subnode ->
                            targetOutNeighborList.get(nodeNeighborListMap.get(node)).contains(subnode)));

            if (!(inRefinable && outRefinable))
                nodesToRemove.get(ii).add(node);

        }));

        //Now remove the nodes from the candidate list

        for (int i = 0; i < candidateList.size(); i++)
            candidateList.get(i).removeAll(nodesToRemove.get(i));


    }

    /**
     * Check if the current candidate list is correct. (i.e., is there any empty candidate list?)
     */
    private boolean isCorrect(List<List<Node>> candidateList) {

        for (List<Node> aCandidateList : candidateList) {
            if (aCandidateList.isEmpty()) return false;
        }

        return true;
    }

    /**
     * Remove redundant appearance of the unique node in the candidate list
     */

    private void removeUniqueNodes(Node unique,
                                   List<List<Node>> candidateList,
                                   int layer) {

        for (int i = layer + 1; i < candidateList.size(); i++) {
            if (candidateList.get(i).contains(unique)) candidateList.get(i).remove(unique);
        }

    }
}
