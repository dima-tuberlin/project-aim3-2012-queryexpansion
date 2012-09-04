package de.tu_berlin.dima.aim3.querysuggestion.livesuggest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 * Class to search deliver query suggestion based on stratosphere query cluster results
 * 
 * @author Michael Huelfenhaus
 * 
 */
public class LiveQuerySuggest {

  // /** Number of search results used .*/
  // private static final int USED_TOP_RESULTS = 5;

  private IndexSearcher idxSearcher = null;

  private Analyzer analyzer = null;

  private QueryParser parser = null;

  /**
   * Get instance of TimeSearcher to search for frequencies of words.
   * 
   * @param _indexDir
   *          Directory of the lucene index
   * @throws IOException
   */
  public LiveQuerySuggest(String _indexDir)
          throws IOException {

    idxSearcher = new IndexSearcher(new SimpleFSDirectory(new File(_indexDir)));

    analyzer = new StandardAnalyzer(IndexUtils.LUCENE_VERSION);

    parser = new QueryParser(IndexUtils.LUCENE_VERSION, null, analyzer);

    System.out.println("Loaded Live Query Suggest for index '" + _indexDir + "'");
  }

  /**
   * Get matching entries for a query
   * 
   * @param _word
   *          word that
   * 
   * @return map of lists of cluster results from index for this query
   * @throws Exception
   */
  public List<List<String>> searchIndex(String query)
          throws Exception {

    Map<Integer, List<String>> suggestionCls = new HashMap<Integer, List<String>>();
    
    Map<Integer,Integer> clusterOrder = new HashMap<Integer, Integer>();
   List<List<String>> suggestionClsLists = new ArrayList<List<String>>();

    String searchCriteria = IndexUtils.KEY_QUERY + ":" + "\"" + query + "\"";

    Query luceneQuery = null;
    try {
      luceneQuery = parser.parse(searchCriteria);
    } catch (ParseException e) {
      System.err.println("Lucene could not parse query: " + searchCriteria);
      e.printStackTrace();

    }
    // TopDocs results = idxSearcher.search(query, 10);

    // TODO sort also by clusterId
    // sort after refinement counts
    Sort clRefSort = new Sort(new SortField[] { new SortField(IndexUtils.KEY_REF_COUNT, SortField.INT, true),
        new SortField(IndexUtils.KEY_CLUSTER_ID, SortField.INT, false) });

    int clusterId;
    String refinement;
    int refCount;

    TopDocs docs = idxSearcher.search(luceneQuery, 1000, clRefSort);
    
    int clusterNum = 0;

    for (ScoreDoc match : docs.scoreDocs) {
      Document d = idxSearcher.doc(match.doc);

      clusterId = (Integer) ((NumericField) d.getFieldable(IndexUtils.KEY_CLUSTER_ID)).getNumericValue();
      refinement = d.get(IndexUtils.KEY_REF);
      refCount = (Integer) ((NumericField) d.getFieldable(IndexUtils.KEY_REF_COUNT)).getNumericValue();

      //  add results to right list
      if (clusterOrder.containsKey(clusterId)) {
        // add to right list
        suggestionClsLists.get(clusterOrder.get(clusterId)).add(refinement);
      } else{
        // add new list
        clusterOrder.put(clusterId,clusterNum);
        suggestionClsLists.add(new ArrayList<String>());
        suggestionClsLists.get(clusterOrder.get(clusterId)).add(refinement);
        clusterNum++;
      }
      
      // add results to map
      if (suggestionCls.containsKey(clusterId)) {

        suggestionCls.get(clusterId).add(refinement);
      } else {
        // for new cluster add new list
        List<String> clRefs = new ArrayList<String>();
        clRefs.add(refinement);
        suggestionCls.put(clusterId, clRefs);

      }

//      System.out.println(clusterId + "\t" + refinement + "\t" + refCount);
    }
    
//    return suggestionCls;
    return suggestionClsLists;

  }

  /**
   * 
   */
  public void liveSuggest() {
    Scanner in = new Scanner(System.in);
    
    String input;

    boolean askUser = true;
    while (askUser){
      System.out.print("Insert query ('' to end): ");
      input = in.nextLine();
      // check if input is empty
      if (!input.equals("")){
        // look for suggestions in index
        try {
          List<List<String>> suggestions = searchIndex(input.trim());
          // output suggestions per cluster sorted by re counts
          for(List<String> cluster : suggestions){
            System.out.print(cluster.get(0));            
//            System.out.print(String.format("10%s", cluster.get(0)));            
            if(cluster.size() > 1){
              System.out.println("\t  " + cluster.subList(1, cluster.size()));
            }else{
              System.out.println();
            }
//            System.out.println(suggestions.get(cluster));
          }
          
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else {
        askUser = false;
      } 
    }
    
    in.close();            
  }

  public static void main(String args[])
          throws Exception {

    LiveQuerySuggest liveSuggest = null;
    try {
      liveSuggest = new LiveQuerySuggest(IndexUtils.INDEX_DIR);
    } catch (IOException e) {
      System.err.println("Could not open Index at: " + IndexUtils.INDEX_DIR);

      e.printStackTrace();
    }
//    System.out.println(liveSuggest.searchIndex("mars"));
    liveSuggest.liveSuggest();
  }

}
