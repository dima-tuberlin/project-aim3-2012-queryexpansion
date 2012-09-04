/***********************************************************************************************************************
 * 
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 **********************************************************************************************************************/

package de.tu_berlin.dima.aim3.querysuggestion;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class QuerySuggTCase
        extends TestBase {

  private static final Log LOG = LogFactory.getLog(QuerySuggTCase.class);

  private String textPath = null;

  private String resultPath = null;

  /** inputfile for test */
  private String inputFile;

  /** file with results for test */
  private String resultfile;

  public QuerySuggTCase(Configuration config) {

    super(config);
  }

  @Override
  protected void preSubmit()
          throws Exception {

    textPath = getFilesystemProvider().getTempDirPath() + "/text";
    resultPath = getFilesystemProvider().getTempDirPath() + "/result";

    System.out.println("Testing");
    
    // delete old test files
    try {
      getFilesystemProvider().delete(textPath, true);
      getFilesystemProvider().delete(resultPath, true);
    } catch (Exception e) {
      // TODO: handle exception
    }
    getFilesystemProvider().createDir(textPath);

    // readlines from test input file
    // String inputText= readLines("/querylog_sample_100.tsv");
    // String inputText= readLines("/querylog_sample_10000.tsv");
    // String inputText= readLines("/querylog_sample_100000.tsv");
    // String inputText = readLines("/querylog_sample_200000.tsv");
    // String inputText= readLines("/user-ct-test-collection-04.txt");

    // String inputText = readLines("/top_ref_text.tsv");
    // String inputText = readLines("/test2_ref_text.tsv");
    // String inputText = readLines("/xanga.tsv");
    // String inputText = readLines("/doc_counts.tsv");

    String inputText = readLines(inputFile);

    // split into file of set length
    int fileLineCount = 1;
    String[] splits = splitInputString(inputText, '\n', fileLineCount);
    int i = 0;
    for (String split : splits) {
      getFilesystemProvider().createFile(textPath + "/part_" + (i++) + ".txt", split);
      LOG.debug("Text Part " + (i - -1) + ":\n>" + split + "<");
    }

  }

  /**
   * Test pipeline until given level with given input file
   * 
   * @param inputFile
   * @param processLevel
   * @throws Exception
   */
  public void basicTest(String inputFile, String resultFile, String processLevel)
          throws Exception {

    this.inputFile = inputFile;
    this.resultfile = resultFile;
    // pre-submit
    preSubmit();

    // submit job
    JobGraph jobGraph = null;
    try {
      jobGraph = getJobGraph(processLevel);
    } catch (Exception e) {
      LOG.error(e);
      Assert.fail("Failed to obtain JobGraph!");
    }

    try {
    	final JobClient client = cluster.getJobClient(jobGraph, getJarFilePath());
		client.submitJobAndWait();
//      cluster. submitJobAndWait(jobGraph, getJarFilePath());
    } catch (Exception e) {
      LOG.error(e);
      Assert.fail("Job execution failed!");
    }

    // post-submit
    postSubmit();
  }

  @Override
  public void testJob()
          throws Exception {

    // // pre-submit
    // preSubmit();
    //
    // // submit job
    // JobGraph jobGraph = null;
    // try {
    // jobGraph = getJobGraph("sessionConstruction");
    // } catch (Exception e) {
    // LOG.error(e);
    // Assert.fail("Failed to obtain JobGraph!");
    // }
    //
    // try {
    // cluster.submitJobAndWait(jobGraph, getJarFilePath());
    // } catch (Exception e) {
    // LOG.error(e);
    // Assert.fail("Job execution failed!");
    // }
    //
    // // post-submit
    // postSubmit();
  }

  @Test
  public void testLogMap()
          throws Exception {
    basicTest("/querysug/input/lineparse.txt", "/querysug/results/mapresults.tsv", "logMap");
//    basicTest("/querysug/input/minimal_test1.tsv", "/querysug/results/mapresults.tsv", "logMap");
    // basicTest("/querysug/input/empty.tsv",
    // "/querysug/results/mapresults.tsv","logMap");
  }

  @Test
  public void testTest()
          throws Exception {

    basicTest("/querysug/input/top_refs_simple.tsv", "/querysug/results/ref_count.tsv", "test");
    // basicTest("/querysug/input/minimal_test1.tsv",
    // "/querysug/results/mapresults.tsv", "logMap");
  }

  @Test
  public void testRefCount()
          throws Exception {

    basicTest("/querysug/input/top_refs_simple.tsv", "/querysug/results/ref_count.tsv", "countRef");
    // basicTest("/querysug/input/minimal_test1.tsv",
    // "/querysug/results/mapresults.tsv", "logMap");
  }

  @Test
  public void testTopRefCount()
          throws Exception {

    basicTest("/querysug/input/porntest.tsv", "/querysug/results/ref_count.tsv", "topCountRef");
    // basicTest("/querysug/input/minimal_test1.tsv",
    // "/querysug/results/mapresults.tsv", "logMap");
  }

  @Test
  public void testSessions()
          throws Exception {
	  
	  basicTest("/querysug/querylog_sample_100.tsv", "/querysug/results/session_results.tsv", "sessionConstruction");
//    basicTest("/querysug/input/sessiontext.tsv", "/querysug/results/session_results.tsv", "sessionConstruction");
  }

  @Test
  public void testSingleSessions()
          throws Exception {

    basicTest("/querysug/input/minimal_test1.tsv", "/querysug/results/session_results.tsv", "singleSessions");
  }


  @Test
  public void testClusterFilteredSessions()
          throws Exception {

    basicTest("/querysug/input/sessions_filtered.tsv", "/querysug/results/session_results.tsv", "sesCluster");
  }
  
  
  @Test
  public void testWhole()
          throws Exception {

    // basicTest("/querysug/input/clusterNumTest2.tsv",
    // "/querysug/results/session_results.tsv", "");

    // basicTest("/querysug/input/porntest.tsv",
    // "/querysug/results/session_results.tsv", "");
//    basicTest("/querysug/querylog_sample_200000.tsv", "/querysug/results/session_results.tsv", "");
    basicTest("/querysug/querylog_sample_10000.tsv", "/querysug/results/session_results.tsv", "");

  }

  @Override
  protected JobGraph getJobGraph()
          throws Exception {

    System.err.println("DONT USE");
    return null;
  }

  protected JobGraph getJobGraph(String processLevel)
          throws Exception {

    // @Override
    // protected JobGraph getJobGraph() throws Exception {

    QuerySuggestClustering wc = new QuerySuggestClustering();
    Plan plan = wc.getPlan(config.getString("QuerySuggestClusteringTest#NoSubtasks", "1"), getFilesystemProvider()
            .getURIPrefix() + textPath, getFilesystemProvider().getURIPrefix() + resultPath, processLevel);
    // config.getString(
    // "QuerySuggestClusteringTest#ProcessLevel", ""));

    PactCompiler pc = new PactCompiler();
    OptimizedPlan op = pc.compile(plan);

    JobGraphGenerator jgg = new JobGraphGenerator();
    return jgg.compileJobGraph(op);

  }

  @Override
  protected void postSubmit()
          throws Exception {

    // Test results
    printResults(resultPath);

    // System.out.println(readLines(resultfile));

    // Test results
    compareResultsByLinesInMemory(readLines(resultfile), resultPath);

    // clean up hdfs
    getFilesystemProvider().delete(textPath, true);
    getFilesystemProvider().delete(resultPath, true);
  }

  @Parameters
  public static Collection<Object[]> getConfigurations() {

    LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

    Configuration config = new Configuration();
    config.setInteger("QuerySuggestClusteringTest#NoSubtasks", 1);
    // config.setInteger("QuerySuggestClusteringTest#NoSubtasks", 4);
    config.setString("QuerySuggestClusteringTest#ProcessLevel", "sessionConstruction");
    tConfigs.add(config);

    return toParameterList(tConfigs);
  }

  public String readLines(String path)
          throws IOException {

    List<String> lines = new ArrayList<String>();
    StringBuffer strBuff = new StringBuffer();
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(path)));
      String line;
      while ((line = reader.readLine()) != null) {
        strBuff.append(line + "\n");
        // lines.add(line);
      }
    } finally {
      if (reader != null) {
        reader.close();// Closeables.closeQuietly(reader);
      }
    }
    return strBuff.toString();
    // return lines;
  }

  /**
   * Print results from the hdfs
   * 
   * @param resultPath
   */
  protected void printResults(String resultPath)
          throws Exception {

    ArrayList<String> resultFiles = new ArrayList<String>();

    // Determine all result files
    if (getFilesystemProvider().isDir(resultPath)) {
      for (String file : getFilesystemProvider().listFiles(resultPath)) {
        if (!getFilesystemProvider().isDir(file)) {
          resultFiles.add(resultPath + "/" + file);
        }
      }
    } else {
      resultFiles.add(resultPath);
    }

    // collect lines of all result files
    PriorityQueue<String> computedResult = new PriorityQueue<String>();
    for (String resultFile : resultFiles) {
      // read each result file
      InputStream is = getFilesystemProvider().getInputStream(resultFile);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();

      // collect lines
      while (line != null) {
        computedResult.add(line);
        line = reader.readLine();
      }
      reader.close();
    }

    // Assert.assertEquals("Computed and expected results have different size",
    // expectedResult.size(), computedResult.size());

    System.out.println("RESULTS:");
    while (!computedResult.isEmpty()) {
      String computedLine = computedResult.poll();
      System.out.println(computedLine);
      // if (LOG.isDebugEnabled())
      // LOG.debug("compLine: <" + computedLine + ">");
      // System.out.println("compLine: <" + computedLine + ">");
      // Assert.assertEquals("Computed and expected lines differ",
      // expectedLine, computedLine);
    }
  }

  private String[] splitInputString(String inputString, char splitChar, int noSplits) {

    String splitString = inputString.toString();
    String[] splits = new String[noSplits];
    int partitionSize = (splitString.length() / noSplits) - 2;

    // split data file and copy parts
    for (int i = 0; i < noSplits - 1; i++) {
      int cutPos = splitString.indexOf(splitChar,
              (partitionSize < splitString.length() ? partitionSize : (splitString.length() - 1)));
      try {
        splits[i] = splitString.substring(0, cutPos) + "\n";
        splitString = splitString.substring(cutPos + 1);
        System.out.println(splitString);
      } catch (Exception e) {
        System.err.println("bad split");
      }
    }
    splits[noSplits - 1] = splitString;

    return splits;
  }

}
