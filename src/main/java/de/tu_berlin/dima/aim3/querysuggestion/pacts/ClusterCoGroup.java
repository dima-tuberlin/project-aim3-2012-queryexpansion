package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tu_berlin.dima.aim3.querysuggestion.IndexMapping;
import de.tu_berlin.dima.aim3.querysuggestion.Matrix;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

public class ClusterCoGroup
        extends CoGroupStub {

  private static final Log LOG = LogFactory.getLog(ClusterCoGroup.class);

  // // TODO parameter
  // /** number of min cluster when reached stop clustering */
  // private final int k = 20;

  private final PactRecord outputRecord = new PactRecord();

  private final PactString query = new PactString();

  private final PactString ref = new PactString();

  private final PactInteger refCount = new PactInteger();

  private final PactInteger clusterId = new PactInteger();

  public static final String MIN_CLUSTER_COUNT = "min_cluster_count";

  public static final String EPSILON = "epsilon";

  public static final String SELF_MULTIPLY = "self_multiply";

  /** number of top entries emitted */
  private int minClusterCount;

  private double epsilon;

  private int selfMultiply;

  /**
   * 
   * 
   * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
   */
  @Override
  public void open(Configuration parameters) {

    this.minClusterCount = parameters.getInteger(MIN_CLUSTER_COUNT, 20);
    // get epsilon value
    String eps = parameters.getString(EPSILON, "0.5");
    this.epsilon = Double.parseDouble(eps);
    this.selfMultiply = parameters.getInteger(SELF_MULTIPLY, 5);

    LOG.info("Clustering for min " + minClusterCount + " Clusters with epsilon: " + epsilon + "self multiply: "
            + selfMultiply);
  }

  @Override
  public void coGroup(Iterator<PactRecord> coOccRecords, Iterator<PactRecord> docRefRecords, Collector<PactRecord> out) {

    IndexMapping refIndexMap = new IndexMapping();
    // own index mapping for doc because urls can be refinements too
    IndexMapping docIndexMap = new IndexMapping();

    // TODO make function to fill matrix with para for columns field
    Matrix normCoOccMatrix = null;
    Matrix normDocMatrix = null;

    double oneMinusEps = 1 - this.epsilon;

    // check if co occurence and doc|ref counts exist
    if (coOccRecords.hasNext() && docRefRecords.hasNext()) {

      // TODO use parameters
      // to save co occurences between refinement of this query
      Matrix coOccMatrix = new Matrix(80, 80);

      // get first entry to get query
      PactRecord coOccRecord = coOccRecords.next();
      String query = coOccRecord.getField(3, PactString.class).getValue();
      // System.out.println();
      // System.out.println("query: " + query);
      String ref1 = coOccRecord.getField(4, PactString.class).getValue();
      String ref2 = coOccRecord.getField(7, PactString.class).getValue();
      int count = coOccRecord.getField(6, PactInteger.class).getValue();
      // add first entry
      coOccMatrix.set(refIndexMap.indexOf(ref1), refIndexMap.indexOf(ref2), count);
      // printMatrix(coOccMatrix);
      // System.out.println("Co Occ for q:  "
      // + coOccRecord.getField(3, PactString.class) + "\tr1: "
      // + coOccRecord.getField(4, PactString.class) + "\tr2:"
      // + coOccRecord.getField(7, PactString.class) + "\t"
      // + coOccRecord.getField(6, PactInteger.class));

      int counter = 0;
      // add all entries to the matrix
      while (coOccRecords.hasNext()) {
        coOccRecord = coOccRecords.next();
        ref1 = coOccRecord.getField(4, PactString.class).getValue();
        ref2 = coOccRecord.getField(7, PactString.class).getValue();
        count = coOccRecord.getField(6, PactInteger.class).getValue();


        // add entry to co occ matrix
        coOccMatrix.set(refIndexMap.indexOf(ref1), refIndexMap.indexOf(ref2), count);

//        System.out.println("Co Occ for q:  " + coOccRecord.getField(3, PactString.class) + "\tr1: "
//        		+ coOccRecord.getField(4, PactString.class) + "\tr2:" + coOccRecord.getField(7, PactString.class)
//        		+ "\t" + coOccRecord.getField(6, PactInteger.class));
        // printMatrix(coOccMatrix);
      }
      // get number real of different refinements to cut matrix to need
      // dimensions
      int realRefCount = refIndexMap.getIndexSize();
      // // cut matrix to needed dimensions
      coOccMatrix = coOccMatrix.getMatrix(0, realRefCount - 1, 0, realRefCount - 1);
      // add matrix with its transpose to get real co occ counts
      coOccMatrix = coOccMatrix.plus(coOccMatrix.transpose());

      // System.out.println(query + "|| coOccMatrix Added transpose");
      // printMatrix(coOccMatrix);

      // TODO move normalisation and t
      // normalize in new matrix by dividing of values with row sums
      normCoOccMatrix = normalizeMatrix(coOccMatrix);

      // System.out.println("Norm Ref Matrix");
      // printMatrix(normCoOccMatrix);
      // TODO make function because also needed for docs

      // make matrix for doc/ref scores
      Matrix docCountMatrix = new Matrix(80, 80 * 15);
      PactRecord docRefRecord;
      String ref;
      String doc;

      // fill doc ref matrix
      while (docRefRecords.hasNext()) {
        docRefRecord = docRefRecords.next();

        ref = docRefRecord.getField(4, PactString.class).getValue();
        doc = docRefRecord.getField(5, PactString.class).getValue();
        count = docRefRecord.getField(6, PactInteger.class).getValue();
        // add entry for doc|ref
        docCountMatrix.set(refIndexMap.indexOf(ref), docIndexMap.indexOf(doc), count);

        // System.out.println("doc|ref for q:  "
        // + docRefRecord.getField(3, PactString.class) + "\tr: "
        // + docRefRecord.getField(4, PactString.class) + "\td:"
        // + docRefRecord.getField(5, PactString.class) + "\t"
        // + docRefRecord.getField(6, PactInteger.class));
        // end debug
      }
      // get number of doc colums by substracting number of refinements
      // form size of index map
      int realDocCount = docIndexMap.getIndexSize();
      // System.out.println("ref " + realRefCount + " doc " + realDocCount);
      // System.out.println(query +"|| pre cut Doc Matrix");
      // printMatrix(docCountMatrix);

      // cut doc ref matrix to needed dimensions
      // try{
      docCountMatrix = docCountMatrix.getMatrix(0, realRefCount - 1, 0, realDocCount - 1);
      // }catch (Exception e) {
      // System.out.println("err");
      // }
      // normalize doc counts
      normDocMatrix = normalizeMatrix(docCountMatrix);

      // System.out.println(query + "|| Norm Doc Matrix");
      // printMatrix(normDocMatrix);

      // normalize with epsilon
      // Multiply a matrix by a scalar in place, A = s*A
      normCoOccMatrix = normCoOccMatrix.timesEquals(oneMinusEps);
      normDocMatrix = normDocMatrix.timesEquals(epsilon);
      // M.timesEquals(eps)
      // M2.timesEquals(1 - eps)

      // // calculate number of by counting rows
      // int refCount = normCoOccMatrix.getRowDimension();
      // // calculate number of different doc by columns count of ref-doc
      // // matrix
      // int docCount = normDocMatrix.getColumnDimension();
      // calculate number for combined probability matrix including
      // columns for f values
      int probColumnCount = realRefCount + realDocCount + 1;
      int docStartColumn = realRefCount;
      int docEndColumn = realRefCount + realDocCount - 1;

      // TODO set good values
      // if (realRefCount > 9) {// && docCount > 3) {

      Matrix probMatrix = new Matrix(realRefCount, probColumnCount);

      // calculate f value by 1 divided by number of different
      // refinements
      double f = 1.0 / realRefCount;
      /** set values in big matrix */
      // set ref co occ count matrix
      probMatrix.setMatrix(0, realRefCount - 1, 0, realRefCount - 1, normCoOccMatrix);

      // System.out.println("first ref co occ");
      // printMatrix(probMatrix);

      // set doc ref count matrix
      probMatrix.setMatrix(0, realRefCount - 1, docStartColumn, docEndColumn, normDocMatrix);

      // System.out.println(query + "|| set doc ref count matrix");
      // printMatrix(probMatrix);

      // make matrix for f column
      Matrix fMatrix = new Matrix(realRefCount, 1, f);

      // set f column
      probMatrix.setMatrix(0, realRefCount - 1, probColumnCount - 1, probColumnCount - 1, fMatrix);
      // System.out.println(" set f column and ");
      // printMatrix(probMatrix);

      // normalize again after adding f
      probMatrix = normalizeMatrix(probMatrix);
      // System.out.println("normalize again after adding f ");
      // printMatrix(probMatrix);

      /** make quadratic matrix for self multiplication */
      Matrix completeMatrix = new Matrix(probColumnCount, probColumnCount);

      // put prob Matrix into complete Matrix
      completeMatrix.setMatrix(0, realRefCount - 1, 0, probColumnCount - 1, probMatrix);

      // add identity matrix to model absorbing doc nodes and absorbing f (topic change)
      completeMatrix.setMatrix(realRefCount, probColumnCount - 1, realRefCount, probColumnCount - 1,
              Matrix.identity(realDocCount +1, realDocCount+1));// TODO
      // change dims
      // System.out.println("Complete Matrix");
//       printMatrix(completeMatrix);
      // multiple with self n times
      for (int i = 0; i < selfMultiply; i++) {
        completeMatrix = completeMatrix.times(completeMatrix);
      }
      // System.out.println(query + "|| complete: after self multiply Matrix");
      // printMatrix(completeMatrix);

      /**
       * complete link clustering
       */
      // map from refs indices to absorption vectors
      Map<Integer, Matrix> vectors = new HashMap<Integer, Matrix>();
      // for saving ids of ref of the clusters
      List<List<Integer>> clusters = new ArrayList<List<Integer>>();
      /** extract vectors and initialize clusters */
      for (int row = 0; row < realRefCount; row++) {
        // System.out.print("row: " + row + " " + refIndexMap.stringOf(row) +"\t");
        // printMatrix(completeMatrix.getMatrix(row, row, docStartColumn, docEndColumn));
        vectors.put(row, completeMatrix.getMatrix(row, row, docStartColumn, docEndColumn));
        // add singleton cluster for this row
        List<Integer> singleCluster = new ArrayList<Integer>();
        singleCluster.add(row);
        clusters.add(singleCluster);
      }
      // System.out.println(clusters);
      // TODO remove or refine
      // if (clusters.size() > 5 && clusters.size() < 10) {

      double maxComplinkScore = -1;
      // cluster until number is k or lower of max complete link score is 0
      while (clusters.size() > minClusterCount && maxComplinkScore != 0) {
        // System.out.println(clusters.size() + " : " + minClusterCount + " :: " + clusters);
        maxComplinkScore = 0;
        double complinkScore;
        // cluster that will be merged
        int mergeCl1 = -1;
        int mergeCl2 = -1;
        // calculate complete link of all pairs of clusters take last element
        for (int clPos1 = clusters.size() - 1; clPos1 != 0; clPos1--) {
          // combine with all preceding
          for (int clPos2 = clPos1 - 1; clPos2 >= 0; clPos2--) {
            complinkScore = completeLinkScore(clusters.get(clPos1), clusters.get(clPos2), vectors);
            // System.out.println("cl score: " + clPos1 + " " + clPos2 +": "+ complinkScore);
            if (complinkScore > maxComplinkScore) {
              maxComplinkScore = complinkScore;
              // get postions of clusters the max complink score
              mergeCl1 = clPos1;
              mergeCl2 = clPos2;
              // System.out.println("higher" + mergeCl1 + " " + mergeCl2);
            }
          }
        }
        // don't merge if max complete link score is 0
        if (maxComplinkScore != 0) {
          // merge clusters with highest complete link score
          clusters.get(mergeCl1).addAll(clusters.get(mergeCl2));
          // delete cluster that was merge into other one
          clusters.remove(mergeCl2);
        }
      }
      // System.out.println("query: " + query);
      // printMatrix(coOccMatrix);
      /** emit clusters */
      // same query for all records
      this.query.setValue(query);
      outputRecord.setField(3, this.query);

      // go over all clusters
      for (int clusterId = 0; clusterId < clusters.size(); clusterId++) {
        List<Integer> cluster = clusters.get(clusterId);
        // go over all entries of the cluster
        for (int clusterEntryPos = 0; clusterEntryPos < cluster.size(); clusterEntryPos++) {
          // todo get count of ref from oc occ matrix row
          int refRow = cluster.get(clusterEntryPos);
          // // TODO remove if keeping join with refcounts
          // double refOccs = sumVector(coOccMatrix.getMatrix(refRow, refRow, 0, realRefCount -
          // 1));
          // System.out.println("cluster : " + clusterId + " " + refIndexMap.stringOf(refRow) +
          // " count: " + refOccs);
          // emit
          this.ref.setValue(refIndexMap.stringOf(refRow));
          // this.refCount.setValue((int) refOccs);
          this.clusterId.setValue(clusterId);

          outputRecord.setField(4, this.ref);
          // outputRecord.setField(6, this.refCount);
          outputRecord.setField(8, this.clusterId);

          out.collect(outputRecord);
        }

      }
      // }

    }
  }

  /**
   * Calculate Complete Link Score for to clusters of vectors
   * 
   * @param cluster1
   * @param cluster2
   * @param vectors
   * @return complete Link score
   */
  private double completeLinkScore(List<Integer> cluster1, List<Integer> cluster2, Map<Integer, Matrix> vectors) {

    double score = Double.MAX_VALUE;
    double cosineSim;
    for (int vectorId1 : cluster1) {
      for (int vectorId2 : cluster2) {
        cosineSim = cosineSimilarity(vectors.get(vectorId1), vectors.get(vectorId2));
        // set complete link score to minimal value
        score = Math.min(score, cosineSim);
      }
    }
    return score;
  }

  /**
   * Calculate cosine similarity of two vectors given as 1 x n matrices
   * 
   * @param vec1
   * @param vec2
   * @return cosine similarity
   */
  private double cosineSimilarity(Matrix vec1, Matrix vec2) {

    double dotProduct = vec1.arrayTimes(vec2).norm1();
    double eucledianDist = vec1.normF() * vec2.normF();
    return dotProduct / eucledianDist;

  }

  /**
   * sum entries of 1 x n matrix
   * 
   * @param vector
   * @return sum of the entries
   */
  private double sumVector(Matrix vector) {

    assert (vector.getRowDimension() == 1);
    double sum = 0;
    for (int j = 0; j < vector.getColumnDimension(); j++) {
      sum += vector.get(0, j);
    }
    return sum;
  }

  /**
   * Calculates the length of the given vector
   * 
   * @param vec
   *          double array of the vector
   * @return length of the vector as a double
   * 
   */
  private static double normF(double[] vec) {

    double norm = 0;
    // calculate sum of squares
    for (int i = 0; i < vec.length; i++) {
      norm += vec[i] * vec[i];
    }
    //
    norm = Math.sqrt(norm);
    return norm;
  }

  /**
   * Normalize Matrix Fields by row sums
   * 
   * @param A
   *          Matrix to normalize
   * @return normalized Matrix
   */
  private Matrix normalizeMatrix(Matrix A) {

    // for every row
    Matrix normMatrix = new Matrix(A.getRowDimension(), A.getColumnDimension());
    double rowSum;
    for (int i = 0; i < A.getRowDimension(); i++) {
      // calculate row sum
      rowSum = 0;
      for (int j = 0; j < A.getColumnDimension(); j++) {
        rowSum += A.get(i, j);
      }

      // divide all values by sum if it is greater 0
      if (rowSum > 0) {
        for (int j = 0; j < A.getColumnDimension(); j++) {
          normMatrix.set(i, j, A.get(i, j) / rowSum);
        }
      }
    }
    return normMatrix;
  }

  /**
   * Print a Matrix
   * 
   * @param A
   */
  private void printMatrix(Matrix A) {

    for (int i = 0; i < A.getRowDimension(); i++) {
      for (int j = 0; j < A.getColumnDimension(); j++) {
        System.out.print(A.get(i, j) + "\t");
      }
      System.out.println();
    }
  }

}
