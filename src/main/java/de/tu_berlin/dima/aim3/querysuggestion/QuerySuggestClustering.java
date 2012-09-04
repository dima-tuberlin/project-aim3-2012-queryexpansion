package de.tu_berlin.dima.aim3.querysuggestion;

import java.io.IOException;
import java.util.Arrays;

import de.tu_berlin.dima.aim3.querysuggestion.pacts.ClusterCoGroup;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.ClusterCountJoinMatcher;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.CoOccRefReducer;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.CountReducer;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.FilterByTopRefMatcher;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.FilterDocsByTopRefs;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.QueryLogMapper;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.SessionMapper;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.SessionReducer;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.SingleRefSesReducer;
import de.tu_berlin.dima.aim3.querysuggestion.pacts.TopCountReducer;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

public class QuerySuggestClustering
        implements PlanAssembler, PlanAssemblerDescription {

  /** stop clustering if this number of clusters is reached*/
  private static final int MIN_CLUSTER_COUNT = 20;

  /** value to normalize ref-ref and ref-doc transition probabilities*/
  private static final String EPSILON = "0.6"; // no set parameter for floaor double

  /** number of steop in random walk equals times the prob. matrix is multiplied with itself*/
  private static final int SELF_MULTIPLY = 4;

  // public static enum ProcessLevel {
  // LOG_MAP, SESSIONS, TOP_REFS
  // }

  // ProcessLevel a = ProcessLevel.LOG_MAP.toString();

  // TODO add as parameter or change to constants
  private static final int TOP_REFS_USED = 80;

  private static final int TOP_DOCS_USED = 15;

// TODO error because of override unclear
//  @Override
  public String getDescription() {

    // TODO Write description
    return "Parameters: [noSubStasks] [input] [output] [processing level]";

  }

  /**
   * Keys for pact records
   * 
   * 0 userid
   * 1 timestamp
   * 2 session id
   * 3 query
   * 4 refinement
   * 5 document
   * 6 count field
   * 7 co occ refinement
   * 8 cluster id
   * 
   * 
   */
  @Override
  public Plan getPlan(String... args) {

    // System.out.println(Arrays.asList(args));
    // parse job parameters
    int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output = (args.length > 2 ? args[2] : "");
    // set how far the pipeline should be processed
    String processingLevel = (args.length > 3 ? args[3] : "");
    // ProcessLevel processLevel = (ProcessLevel) processingLevel;

    // sink to write results
    FileDataSink out = null;

    FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Query Log Lines");
    // TODO test
    // source.setDegreeOfParallelism(noSubTasks);
    MapContract logMapper = new MapContract(QueryLogMapper.class, source, "Filter Query Log Lines");
    logMapper.setDegreeOfParallelism(noSubTasks);
    //
    // key is userId
    SingleInputContract sessions;
//    ReduceContract sessions;

    // to use session data directly only works with session files!
    if(!processingLevel.equals("sesCluster")){
    sessions = new ReduceContract(SessionReducer.class, PactInteger.class, 0, logMapper,
            "Find sessions");
    sessions.setDegreeOfParallelism(noSubTasks);
    } else{
      sessions = new MapContract(SessionMapper.class, source, "Filter Query Log Lines");
    }

      
    
    Class[] ClassArray3String = { PactString.class, PactString.class, PactString.class };
    int[] sesQueRefFields = { 2, 3, 4 };
    // make sure every ref in only once in every session
    ReduceContract singleRefSesReducer = new ReduceContract(SingleRefSesReducer.class, ClassArray3String,
            sesQueRefFields, sessions, "single Refs per session");
    singleRefSesReducer.setDegreeOfParallelism(noSubTasks);
    // count ref occ per query
    Class[] key2StringClasses = { PactString.class, PactString.class };
    // key are query and refinement
    int[] keyFields = { 3, 4 };
    ReduceContract countRefReducer = new ReduceContract(CountReducer.class, key2StringClasses, keyFields,
            singleRefSesReducer, "Count Refs");
    countRefReducer.setDegreeOfParallelism(noSubTasks);
    // find top n refinements
    ReduceContract topRefReducer = new ReduceContract(TopCountReducer.class, PactString.class, 3, countRefReducer,
            "Find top refs");
    topRefReducer.setDegreeOfParallelism(noSubTasks);
    // use only top n ref per query

    topRefReducer.setParameter(TopCountReducer.MAX_TOP_COUNT, TOP_REFS_USED);
    // FileDataSink out = new FileDataSink(RefCountOutputFormat.class,
    // output, topRefReducer,"out");

    // // first filter session entries by top refs
    int[] KeyQueryRef = { 3, 4 };
    MatchContract filterByTopRefs = new MatchContract(FilterByTopRefMatcher.class, key2StringClasses, KeyQueryRef,
            KeyQueryRef, singleRefSesReducer, topRefReducer, "Filter Session Entries by Top Ref");
    // set delete field to doc field
    filterByTopRefs.setParameter(FilterDocsByTopRefs.DEL_FIELD, 5);
    // filterByTopRefs.setParameter(FilterByTopRefMatcher.DEL_FIELD, 5);
    filterByTopRefs.setDegreeOfParallelism(noSubTasks);

    // FileDataSink out = new FileDataSink(SessionOutputFormat.class,
    // output,
    // filterByTopRefs,"out");
    //
    // /** start ref co occ counting */
    int[] coOccKeyFields = { 2, 3 };
    ReduceContract coOccRefReducer = new ReduceContract(CoOccRefReducer.class, key2StringClasses, coOccKeyFields,
            filterByTopRefs, "Find cooccuring refinements");
    coOccRefReducer.setDegreeOfParallelism(noSubTasks);
    // sum co occurences
    int[] sumCoOccKeyFields = { 3, 4, 7 };

    ReduceContract coOccCountReducer = new ReduceContract(CountReducer.class, ClassArray3String, sumCoOccKeyFields,
            coOccRefReducer, "Sum ref co occ");
    coOccCountReducer.setDegreeOfParallelism(noSubTasks);
    // /** oc cocc output*/
    // FileDataSink out = new FileDataSink(CoOccCountOutputFormat.class,
    // output, coOccCountReducer, "out");

    /** counting documents */
    // filter session entries by top n ref
    MatchContract filterDocsByTopRefs = new MatchContract(FilterByTopRefMatcher.class, key2StringClasses, KeyQueryRef,
            KeyQueryRef, sessions, topRefReducer, "Filter Session for doc Entries by Top Ref");
    // set parameter to delete session id filed
    filterDocsByTopRefs.setParameter(FilterDocsByTopRefs.DEL_FIELD, 2);
    filterDocsByTopRefs.setDegreeOfParallelism(noSubTasks);

    int[] KeyQueryRefDoc = { 3, 4, 5 };
    // count doc per ref
    ReduceContract countDocPerRefReducer = new ReduceContract(CountReducer.class, ClassArray3String, KeyQueryRefDoc,
            filterDocsByTopRefs, "Count Docs per Refs");
    countDocPerRefReducer.setDegreeOfParallelism(noSubTasks);

    // find top n docs per ref
    ReduceContract topDocPerRefReducer = new ReduceContract(TopCountReducer.class, ClassArray3String, KeyQueryRefDoc,
            countDocPerRefReducer, "Find top n docs per refs with count");
    // use only top n docs per ref
    topDocPerRefReducer.setParameter(TopCountReducer.MAX_TOP_COUNT, TOP_DOCS_USED);
    topDocPerRefReducer.setDegreeOfParallelism(noSubTasks);

    /** end counting documents output d/r = c for top d of top r */
    // combine doc counts and co ref
    CoGroupContract clusterCoGroup = new CoGroupContract(ClusterCoGroup.class, PactString.class, 3, 3,
            coOccCountReducer, topDocPerRefReducer, "Cluster");
    clusterCoGroup.setDegreeOfParallelism(noSubTasks);
    // clustering parameters
    clusterCoGroup.setParameter(ClusterCoGroup.MIN_CLUSTER_COUNT, MIN_CLUSTER_COUNT);
    clusterCoGroup.setParameter(ClusterCoGroup.EPSILON, EPSILON);
    clusterCoGroup.setParameter(ClusterCoGroup.SELF_MULTIPLY, SELF_MULTIPLY);

    int[] joinClustersWithCountsKeyFields = { 3, 4 };
    MatchContract joinClustersWithCounts = new MatchContract(ClusterCountJoinMatcher.class, key2StringClasses,
            joinClustersWithCountsKeyFields, joinClustersWithCountsKeyFields, clusterCoGroup, topRefReducer,
            "Join clusters with Top Ref counts");
    joinClustersWithCounts.setDegreeOfParallelism(noSubTasks);

    // set output level and format
    if (processingLevel.equals("logMap")) {
      out = new FileDataSink(LogMapperOutputFormat.class, output, logMapper, "Filtered Query log entries");
    } else if (processingLevel.equals("test")) {
      out = new FileDataSink(TestOutputFormat.class, output, logMapper, "test");
    } else if (processingLevel.equals("sessionConstruction")) {
      out = new FileDataSink(SessionOutputFormat.class, output, sessions, "Session records");
    } else if (processingLevel.equals("singleSessions")) {
      out = new FileDataSink(SessionOutputFormat.class, output, singleRefSesReducer, "Single Session records");
    } else if (processingLevel.equals("countRef")) {
      out = new FileDataSink(RefCountOutputFormat.class, output, countRefReducer, "count ref records");
    } else if (processingLevel.equals("topCountRef")) {
      out = new FileDataSink(RefCountOutputFormat.class, output, topRefReducer, "top count ref records");
    } else if (processingLevel.equals("cluster")) {
      out = new FileDataSink(ClusterOutputFormat.class, output, clusterCoGroup,
              "cluster result with count from co occ matrix");
    } else {
      out = new FileDataSink(ClusterOutputFormat.class, output, joinClustersWithCounts, "clusters with top ref counts");
    }

    // FileDataSink out = new FileDataSink(SessionOutputFormat.class,
    // output,
    // filterByTopRefs,"out");

    // FileDataSink out = new FileDataSink(RefCountOutputFormat.class,
    // output,
    // topRefReducer,"out");

    //
    // FileDataSink out = new FileDataSink(RefCountOutputFormat.class,
    // output,
    // topRefReducer, "out");

    // source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII"); //
    // comment out this line for UTF-8 inputs

    Plan plan = new Plan(out, "Session Finding Example");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }

  /**
   * Writes <tt>PactRecord</tt> containing sessionID, query, refinement, document
   */
  public static class SessionOutputFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void writeRecord(PactRecord record)
            throws IOException {

      this.buffer.setLength(0);
      this.buffer.append(record.getField(2, PactString.class).getValue());
      this.buffer.append('\t');
      this.buffer.append(record.getField(3, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(4, PactString.class).getValue());
      this.buffer.append('\t');
      this.buffer.append(record.getField(5, PactString.class).getValue());
      this.buffer.append('\t');
      this.buffer.append(record.getField(6, PactInteger.class).getValue());
      this.buffer.append('\n');

      byte[] bytes = this.buffer.toString().getBytes();
      this.stream.write(bytes);
    }
  }

  /**
   * Writes <tt>PactRecord</tt> containing 3 entries query , ref, count
   */
  public static class RefCountOutputFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void writeRecord(PactRecord record)
            throws IOException {

      this.buffer.setLength(0);
      this.buffer.append("q: ");

      this.buffer.append(record.getField(3, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append("r: ");
      this.buffer.append(record.getField(4, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(6, PactInteger.class).getValue());
      this.buffer.append('\n');
      byte[] bytes = this.buffer.toString().getBytes();
      this.stream.write(bytes);
    }

  }

  /**
   * Writes <tt>PactRecord</tt> containing 3 entries query , ref, count
   */
  public static class CoOccCountOutputFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void writeRecord(PactRecord record)
            throws IOException {

      this.buffer.setLength(0);
      this.buffer.append(record.getField(3, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(4, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(7, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(6, PactInteger.class).getValue());
      this.buffer.append('\n');
      byte[] bytes = this.buffer.toString().getBytes();
      this.stream.write(bytes);
    }

  }

  /**
   * Writes <tt>PactRecord</tt> containing
   */
  public static class LogMapperOutputFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void writeRecord(PactRecord record)
            throws IOException {

      this.buffer.setLength(0);
      // userid
      this.buffer.append(record.getField(0, PactInteger.class).toString());
      this.buffer.append('\t');
      // time as long
      this.buffer.append(record.getField(1, PactLong.class).toString());
      this.buffer.append('\t');
      // query
      this.buffer.append(record.getField(3, PactString.class).toString());
      this.buffer.append('\t');
      // doc
      this.buffer.append(record.getField(5, PactString.class).toString());
      this.buffer.append('\n');
      byte[] bytes = this.buffer.toString().getBytes();
      this.stream.write(bytes);
    }
  }

  /**
   * Writes <tt>PactRecord</tt> containing
   */
  public static class GeneralOutputFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void writeRecord(PactRecord record)
            throws IOException {

      this.buffer.setLength(0);

      this.buffer.append(record.getField(2, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(3, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(4, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(6, PactInteger.class).getValue());
      this.buffer.append('\n');
      byte[] bytes = this.buffer.toString().getBytes();
      this.stream.write(bytes);
    }
  }

  /**
   * Writes <tt>PactRecord</tt> containing
   */
  public static class TestOutputFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void writeRecord(PactRecord record)
            throws IOException {

      this.buffer.setLength(0);
      // userid
      this.buffer.append(record.getField(0, PactString.class).toString());
      this.buffer.append('\n');
      byte[] bytes = this.buffer.toString().getBytes();
      this.stream.write(bytes);
    }
  }

  /**
   * Writes <tt>PactRecord</tt> containing
   * 
   * Output 
   * query, clusterID, refinement, refinementCount
   */
  public static class ClusterOutputFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void writeRecord(PactRecord record)
            throws IOException {

      this.buffer.setLength(0);

      this.buffer.append(record.getField(3, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(8, PactInteger.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(4, PactString.class).toString());
      this.buffer.append('\t');
      this.buffer.append(record.getField(6, PactInteger.class).getValue());
      this.buffer.append('\n');
      byte[] bytes = this.buffer.toString().getBytes();
      this.stream.write(bytes);
    }
  }

  public static class EdgeInFormat
          extends DelimitedInputFormat {

    private final PactString rdfSubj = new PactString();

    private final PactString rdfPred = new PactString();

    private final PactString rdfObj = new PactString();

    @Override
    public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {

      int startPos = 0;
      startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, numBytes, ' ', rdfSubj, '"');
      if (startPos < 0)
        return false;
      startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, numBytes, ' ', rdfPred, '"');
      if (startPos < 0 || !rdfPred.getValue().equals("<http://xmlns.com/foaf/0.1/knows>"))
        return false;
      startPos = parseVarLengthEncapsulatedStringField(bytes, startPos, numBytes, ' ', rdfObj, '"');
      if (startPos < 0)
        return false;

      if (rdfSubj.compareTo(rdfObj) <= 0) {
        target.setField(0, rdfSubj);
        target.setField(1, rdfObj);
      } else {
        target.setField(0, rdfObj);
        target.setField(1, rdfSubj);
      }

      System.out.println(target.getField(0, PactString.class).getValue() + " - "
              + target.getField(1, PactString.class).getValue());

      return true;
    }

    private int parseVarLengthEncapsulatedStringField(byte[] bytes, int startPos, int length, char delim,
            PactString field, char encaps) {

      boolean isEncaps = false;

      if (bytes[startPos] == encaps) {
        isEncaps = true;
      }

      if (isEncaps) {
        // encaps string
        for (int i = startPos; i < length; i++) {
          if (bytes[i] == encaps) {
            if (bytes[i + 1] == delim) {
              field.setValueAscii(bytes, startPos, i - startPos + 1);
              return i + 2;
            }
          }
        }
        return -1;
      } else {
        // non-encaps string
        int i;
        for (i = startPos; i < length; i++) {
          if (bytes[i] == delim) {
            field.setValueAscii(bytes, startPos, i - startPos);
            return i + 1;
          }
        }
        if (i == length) {
          field.setValueAscii(bytes, startPos, i - startPos);
          return i + 1;
        } else {
          return -1;
        }
      }
    }
  }


}