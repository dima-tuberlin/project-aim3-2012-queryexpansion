package de.tu_berlin.dima.aim3.querysuggestion.livesuggest;

import java.io.File;
import java.util.HashSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.util.Version;

public class IndexUtils {

  /** System independent separator character for paths. */
  public static final char SEP = File.separatorChar;

  /** Directory where output is written. */
  public static final String OUTPUT_DIR = "." + SEP + "output" + SEP;
  
  /** Directory of the lucene index */
  public static final String INDEX_DIR = OUTPUT_DIR + SEP + "index";
  
//  public static final String TEST_INDEX_DIR = OUTPUT_DIR + SEP + "testindex";
  
//  /** Directory of the after the pipeline */
  public static final String RESULT_DIR = "." + SEP + "results" + SEP;
  
  /** Used Lucene Version */
  public static final Version LUCENE_VERSION = Version.LUCENE_34;
  
  /** Lucene key for text where search is performed . */
  public static final String KEY_QUERY = "query";

  /** Lucene key for the json. */
  public static final String KEY_REF = "refinement";
  /** gigaword id of source article */  
  public static final String KEY_CLUSTER_ID = "clusterId";
  
  public static final String KEY_REF_COUNT = "refCount";
    
}
