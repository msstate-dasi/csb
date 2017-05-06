package subgraphIso;

import java.util.*;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Subgraph Isomorphism
 * A Java Plugin for Neo4j
 *
 *
 */
public class Subgraph_Isomorphism
{
    // Only static fields and @Context-annotated fields are allowed in
    // Procedure classes. This static field is the configuration we use
    // to create full-text indexes.
    private static final Map<String,String> FULL_TEXT =
            stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" );

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;


    @Procedure("subgraphIso")
    @Description("Execute lucene query in the given index, return found nodes")
    public Stream<result> subgraphIso( @Name("query") String query, @Name("target") String target )
    {

        Label queryLabel=Label.label(query);
        Label targetLabel=Label.label(target);
        ArrayList<Node> candidateListIndex=new ArrayList<>();
        List<List<Node>> matchedSubgraphs = UllmannAlg(queryLabel, targetLabel,candidateListIndex);
        ArrayList<result> resultList=new ArrayList<>();

        try {
            //execute the algorithm, update the candidateListIndex and return the result
            //each row of the matchedSubgraphs contains nodes in a matched subgraph ordered by the query nodes in the candidateListIndex
            //i.e., candidateListIndex.size()==matchedSubgraphs.get(i).size();

            if (matchedSubgraphs.isEmpty())
                return null;
            else {
                    ArrayList<Node> resultNodes=new ArrayList<>();

                    for(int i=0;i<matchedSubgraphs.size();i++)
                    {
                        for(int j=0;j<matchedSubgraphs.get(i).size();j++)
                        {
                            resultList.add(new result(matchedSubgraphs.get(i).get(j),
                                    candidateListIndex.get(j),
                                    Integer.toString(i),
                                    Integer.toString(matchedSubgraphs.size())));

                        }


                    }

                    //Note: the result objects can only have String or Node type instance variables.
                    return resultList.stream();
                }
            } catch (Exception e) {
                String errMsg = "Error encountered while calculating subgraph isomorphism";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);}
   }





    private List<List<Node>> UllmannAlg(Label queryLabel, Label targetLabel,ArrayList<Node> candidateListIndex){

        List<List<Node>> candidateList=findCandidates(queryLabel,targetLabel,candidateListIndex);//create the candidate list and index
        List<List<Node>> queryNeighborList=new ArrayList<>();// the neighbor list for query vertices
        List<List<Node>> nodeNeighborList=new ArrayList<>();// the neighbor list for the Neo4j database with the target label
        List<List<Node>> matchedSubgraphs=new ArrayList<>();//store the final results


        //sort the candidate list by the number of candidates that a node has.
        Comparator<List<Node>> candidateListSizeComparator=new Comparator<List<Node>>() {
            @Override
            public int compare(List<Node> o1, List<Node> o2) {
                return o1.size()-o2.size();
            }
        };
        candidateList.sort(candidateListSizeComparator);

        //create the query graph's neighbor list
        ResourceIterator<Node> queryNodes=db.findNodes(queryLabel);

        while(queryNodes.hasNext())
        {
            Node queryNode=queryNodes.next();
            queryNeighborList.add(findNodeNeighbors(queryNode));
        }

        queryNodes.close();

        //create the neighbor list for the Neo4j database with the target label
        ResourceIterator<Node> targetNodes;
        ArrayList<Node> nodeNeighborListIndex=new ArrayList<>();
        if(targetLabel.name().equals("All"))
            targetNodes=db.getAllNodes().iterator();
        else
            targetNodes=db.findNodes(targetLabel);

        while(targetNodes.hasNext())
        {
            Node targetNode=targetNodes.next();

            //pass the query graph nodes if all neo4j database is selected
            if (targetLabel.name().equals("All") && targetNode.hasLabel(queryLabel))
                continue;

            nodeNeighborList.add(findNodeNeighbors(targetNode));
            nodeNeighborListIndex.add(targetNode);
        }

        targetNodes.close();


        refineCandidate(candidateList,queryNeighborList,nodeNeighborList,candidateListIndex,nodeNeighborListIndex);//the first round refine
        if(!isCorrect(candidateList))
        {
            System.out.println("No subgraphs found!");
            return matchedSubgraphs;
        }
        backtracking(0,candidateList,queryNeighborList,nodeNeighborList,candidateListIndex,nodeNeighborListIndex,matchedSubgraphs);



        return matchedSubgraphs;

    }

    private boolean backtracking(int numLayer, List<List<Node>> candidateList,List<List<Node>> queryNeighborList,
                                 List<List<Node>> nodeNeighborList, ArrayList<Node> candidateListIndex,
                                 ArrayList<Node> nodeNeighborListIndex,List<List<Node>> matchedSubgraphs)
    {

        if(numLayer==queryNeighborList.size())
        {
            System.out.println("Subgraph found!");
            ArrayList<Node> subgraph=new ArrayList<>();

            for(int i=0;i<candidateList.size();i++)
            {
                subgraph.add(candidateList.get(i).get(0));

            }
            matchedSubgraphs.add(subgraph);
            return true;
        }

        List<List<Node>> originalCandidateList= copyNodeList(candidateList);
        //retain the original copy of the candidateList for rolling back
        for(int i=0;i<candidateList.get(numLayer).size();i++)
        {

            if(numLayer==0&&i==10)return true;

            ArrayList<Node> singleNode=new ArrayList<>();
            singleNode.add(candidateList.get(numLayer).get(i));

            candidateList.get(numLayer).retainAll(singleNode);//select the single node in that row

            removeUniqueNodes(singleNode.get(0),candidateList,numLayer,false);//remove unique nodes in other rows
            refineCandidate(candidateList,queryNeighborList,nodeNeighborList,candidateListIndex,nodeNeighborListIndex);//refine the candidate list
            if(isCorrect(candidateList))
                backtracking(numLayer+1,candidateList,queryNeighborList,nodeNeighborList,candidateListIndex,nodeNeighborListIndex,matchedSubgraphs);

            removeUniqueNodes(originalCandidateList.get(numLayer).get(i), originalCandidateList, numLayer + 1,true);//trim the branches

            candidateList=copyNodeList(originalCandidateList);//resume the candidate list and continue searching

        }

        return false;
    }

    private void removeUniqueNodes(Node unique, List<List<Node>> candidateList,int layer,boolean TrimBranch)
    {

        if(!TrimBranch) {
            for (int i = layer+1; i < candidateList.size(); i++) {
                if (candidateList.get(i).contains(unique) ) {
                    candidateList.get(i).remove(unique);
                }
            }
        }
        else{

            for(int i=layer;i<candidateList.size();i++)
            {
                if(candidateList.get(i).contains(unique) && candidateList.get(i).size()>1)
                    candidateList.get(i).remove(unique);
            }
        }
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

    private boolean isCorrect(List<List<Node>> candidateList)
    {//check if the current candidate list is correct. (i.e., is there any empty candidate list?)
        for(int i=0;i<candidateList.size();i++)
        {
            if(candidateList.get(i).isEmpty()) return false;

        }
        return true;

    }


    private void refineCandidate(List<List<Node>> candidateList,List<List<Node>> queryNeighborList,List<List<Node>> nodeNeighborList, ArrayList<Node> candidateListIndex, ArrayList<Node> nodeNeighborListIndex)
    {//given the three lists, refine the candidate list
        for(int i=0;i<queryNeighborList.size();i++)
        {
            for(int j=0;j<candidateList.get(i).size();j++)
            {
                boolean refinable=true;
                List<Node> subCandidateList;//candidates of a neighbor of the current vertex i c(n(x))

                //find the neighbors of a candidate n(c(x))
                List<Node> subNodeNeighborList;
                int nodeNeighborIndex=nodeNeighborListIndex.indexOf(candidateList.get(i).get(j));

                subNodeNeighborList=nodeNeighborList.get(nodeNeighborIndex);

                for (int k=0;k<queryNeighborList.get(i).size();k++)
                {
                    boolean partialRefinable=false;

                    //find the candidates of a neighbor of the current vertex i c(n(x))
                    int candidateIndex=candidateListIndex.indexOf(queryNeighborList.get(i).get(k));

                    subCandidateList=candidateList.get(candidateIndex);
                    for(int l=0;l<subCandidateList.size();l++)
                    {
                        if(subNodeNeighborList.contains(subCandidateList.get(l)))
                        // check if n(c(x)) contains at least an element from c(n(x))
                        {
                            partialRefinable=true;
                            break;
                        }
                    }
                    if(!partialRefinable)
                    {
                        refinable=false;
                        break;
                    }
                }
                if(!refinable)
                {

                    candidateList.get(i).remove(j);
                }

            }
        }

    }


    private List<List<Node>> findCandidates(Label queryLabel, Label targetLabel, ArrayList<Node> candidateListIndex)
    {//find all query vertex candidates in the Neo4j database under the label "targetLabel"
        //input an empty candidate-list index list for modification
        List<List<Node>> candidateList=new ArrayList<>();


        ResourceIterator<Node> queryNodes=db.findNodes(queryLabel);
        ResourceIterator<Node> targetNodes;

        while(queryNodes.hasNext())
        {
            ArrayList<Node> candidateListPerVertex = new ArrayList<>();
            Node queryNode=queryNodes.next();

            if (targetLabel.name().equals("All"))
                targetNodes = db.getAllNodes().iterator();
            else
                targetNodes=db.findNodes(targetLabel);

            while(targetNodes.hasNext())
            {
                Node targetNode=targetNodes.next();

                //pass the nodes in the query graph
                if (targetLabel.name().equals("All") && targetNode.hasLabel(queryLabel))
                    continue;

                if(targetNode.getDegree()>=queryNode.getDegree())
                {
                    candidateListPerVertex.add(targetNode);
                }
            }
            candidateListIndex.add(queryNode);
            candidateList.add(candidateListPerVertex);
            System.out.println("# of candidates for query vertex " + queryNode + ": " + candidateListPerVertex.size());
            targetNodes.close();

        }
        queryNodes.close();

        System.out.println("# of query vertices in the candidate list: "+candidateList.size());

        return candidateList;
    }

    private ArrayList<Node> findNodeNeighbors(Node node)
    {//find the neighbors of a node in the Neo4j database
        ArrayList<Node> neighborsOfnode=new ArrayList<>();
        Iterator<Relationship> relationships=node.getRelationships().iterator();
        while(relationships.hasNext()){
            neighborsOfnode.add(relationships.next().getOtherNode(node));
        }
        return neighborsOfnode;
    }



}
