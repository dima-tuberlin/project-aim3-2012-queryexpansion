package de.tu_berlin.dima.aim3.querysuggestion.livesuggest;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 * Class to write data from UIMA pipeline to a lucene index
 * 
 * How to add data to a timeline index
 * 
 * 1. make index and provide path where the index should be saved 2. open index 3. add timeline
 * result 4. close index (in finally block) 5. optional: optimize index takes long for big indices
 * 
 * @author Michael Huelfenhaus
 * 
 */
public class SuggestIndexer {

  /** Analyzes words */
  private final Analyzer analyzer;

  private final Directory indexDirectory;

  private IndexWriter writer;

  /**
   * Creates a new instance of the TimeIndexer.
   * 
   * @param _indexDir
   *          Directory for saving the index
   * @throws IOException
   */
  public SuggestIndexer(String _indexDir)
          throws IOException {

    indexDirectory = new SimpleFSDirectory(new File(_indexDir));
    analyzer = new StandardAnalyzer(IndexUtils.LUCENE_VERSION);
    writer = null;
  }

  /**
   * Open index.
   */
  public void openIndex() {

    IndexWriterConfig config = new IndexWriterConfig(IndexUtils.LUCENE_VERSION, analyzer);
    try {
      writer = new IndexWriter(indexDirectory, config);
    } catch (CorruptIndexException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (LockObtainFailedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Close index.
   */
  public void closeIndex() {

    try {
      writer.close();
    } catch (CorruptIndexException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Create lucene document for a result from the timeline processing pipeline.
   * 
   * @param result
   *          timeline result for a found event
   * @return
   */
  public Document createDocument(String query, int clusterID, String refinement, int refCount) {

    Document document = new Document();

    // save and index query
    document.add(new Field(IndexUtils.KEY_QUERY, query, Field.Store.YES, Field.Index.NOT_ANALYZED));

    // save clusterId as numeric
    document.add(new NumericField(IndexUtils.KEY_CLUSTER_ID,Field.Store.YES, false).setIntValue(clusterID));

    // save and index the refinement
    document.add(new Field(IndexUtils.KEY_REF, refinement, Field.Store.YES, Field.Index.NOT_ANALYZED));

    // save and index the refinement count as numeric
    document.add(new NumericField(IndexUtils.KEY_REF_COUNT,Field.Store.YES, true).setIntValue(refCount));
    
    System.out.println(document);
    return document;
  }

  /**
   * Delete index by overwriting with a new empty index.
   * 
   * @throws IOException
   */
  public void deleteIndex()
          throws IOException {

    System.out.println("Index deleted");
    IndexWriterConfig config = new IndexWriterConfig(IndexUtils.LUCENE_VERSION, analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    IndexWriter idxWriter = new IndexWriter(indexDirectory, config);
    idxWriter.close();
  }

  // /**
  // * Get an lucene index writer.
  // *
  // * @return a index writer
  // *
  // * @throws CorruptIndexException
  // * @throws LockObtainFailedException
  // * @throws IOException
  // */
  // public IndexWriter getWriter()
  // throws CorruptIndexException, LockObtainFailedException, IOException {
  //
  // if (this.idxWriterPublic == null) {
  // IndexWriterConfig config = new
  // IndexWriterConfig(IndexUtils.LUCENE_VERSION,
  // analyzer);
  // IndexWriter idxWriter = new IndexWriter(indexDirectory, config);
  // this.idxWriterPublic = idxWriter;
  // }
  // return this.idxWriterPublic;
  // }

  /**
   * Optimize the lucene index to make it smaller and faster.
   * 
   * @throws IOException
   */
  public void optimiseIndex()
          throws IOException {

    System.out.println("Optimzing index, this might take a while.");
    openIndex();
    // Optimize and close index
    writer.optimize();
    // idxWriter.close();
    closeIndex();
  }

  /**
   * Add results from the stratosphere query clustering Pact program pipeline to the index
   * 
   * @param clusterFilesPath
   *          Path to the file that resulted from the stratosphere query clustering Pact program
   * 
   *          Format: query, clusterID, refinement, refinementCount tab-separated
   * @throws IOException
   */
  public void addFilesToIndex(String clusterFilesPath)
          throws IOException {

    IndexWriterConfig config = new IndexWriterConfig(IndexUtils.LUCENE_VERSION, analyzer);
    IndexWriter idxWriter = new IndexWriter(indexDirectory, config);

    File[] files = IOUtils.listTypFiles(clusterFilesPath, "txt");
    for (File file : files) {

      BufferedReader reader = IOUtils.getUtf8FileReader(file);
      // System.out.println("Start adding " + uimaFilesPath);

      String query;
      int clusterId;
      String refinement;
      int refCount;

      String line;
      // for every entry make a lucene document and add it to the index
      while ((line = reader.readLine()) != null) {
        String[] fields = line.split("\t");
        query = fields[0];
        clusterId = Integer.parseInt(fields[1]);
        refinement = fields[2];
        refCount = Integer.parseInt(fields[3]);
        idxWriter.addDocument(createDocument(query, clusterId, refinement, refCount));
      }
    }

    // close index
    idxWriter.close();
  }

  /**
   * Test writing and creating an index.
   * 
   * @param args
   */
  public static void main(String args[]) {

    SuggestIndexer indexer = null;

    // create indexer
    try {
      indexer = new SuggestIndexer(IndexUtils.INDEX_DIR);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Could not open Index at: " + IndexUtils.INDEX_DIR);
    }
    
    // delete index old index
    try {
      indexer.deleteIndex();
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Could not delete Index at: " + IndexUtils.INDEX_DIR);
    }

    try {
      indexer.addFilesToIndex(IndexUtils.RESULT_DIR);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
//    indexer.openIndex();
//    try {
//      indexer.addResultToIndex("query","1","refinement","4");
//
//    } catch (CorruptIndexException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    } catch (IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    } finally {
//      indexer.closeIndex();
//    }

  }

}
