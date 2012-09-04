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

package de.tu_berlin.dima.aim3.querysuggestion.stats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Implements a word count which takes the input file and counts the number of the occurrences of
 * each word in the file.
 * 
 * @author Larysa, Moritz Kaufmann, Stephan Ewen
 * @author Michael Huelfenhaus
 */
public class LogFieldCount
        implements PlanAssembler, PlanAssemblerDescription {

  /**
   * Writes <tt>PactRecord</tt> containing an string (word) and an integer (count) to a file. The
   * output format is: "&lt;word&gt; &lt;count&gt;\n"
   */
  public static class CountOutFormat
          extends FileOutputFormat {

    private final StringBuilder buffer = new StringBuilder();

//    @Override
    public void writeRecord(PactRecord record)
            throws IOException {
      int count = record.getField(6, PactInteger.class).getValue();
      // TODO parameter
      // filter by counts over 100
      if (count > 1000) {        

        this.buffer.setLength(0);
        for(int i = 0;i < (6 - Integer.toString(count).length()) ;i++){
          this.buffer.append(' ');
        }
        this.buffer.append(count);
        this.buffer.append('\t');
        this.buffer.append(record.getField(0, PactString.class).toString());
        this.buffer.append('\n');

        byte[] bytes = this.buffer.toString().getBytes();
        this.stream.write(bytes);
      }
    }
  }

  public static class QueryLogMapper
          extends MapStub {

    private static final Log LOG = LogFactory.getLog(QueryLogMapper.class);

    private static final String EMPTY_QUERY = "-";

    private static final String COUNT_FIELD = "parameter.countField";

    private final PactRecord outputRecord = new PactRecord();

    private final PactString key = new PactString();

    private final PactInteger oneCount = new PactInteger(1);

    private int countField;

    /**
     * 
     * 
     * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
     */
    @Override
    public void open(Configuration parameters) {

      this.countField = parameters.getInteger(COUNT_FIELD, 0);
    }

    @Override
    public void map(PactRecord record, Collector<PactRecord> out)
            throws Exception {

      // the query log line is the first field from the record of the
      // TextInputFormat
      String[] fields = record.getField(0, PactString.class).toString().split("\t");

      if (!fields[1].equals(EMPTY_QUERY)) {
        key.setValue(fields[countField]);
        outputRecord.setField(0, key);
        outputRecord.setField(6, oneCount);
        // emit valid log entry
        out.collect(outputRecord);

      }

    }

  }

  /**
   * Sums up the counts for a certain given key. The counts are assumed to be at position
   * <code>1</code> in the record. The other fields are not modified.
   */
  @Combinable
  public static class CountKeys
          extends ReduceStub {

    private final PactInteger theInteger = new PactInteger();

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
            throws Exception {

      PactRecord element = null;
      int sum = 0;
      while (records.hasNext()) {
        element = records.next();
        PactInteger i = element.getField(6, PactInteger.class);
        sum += i.getValue();
      }

      this.theInteger.setValue(sum);
      element.setField(6, this.theInteger);
      out.collect(element);
    }

    /*
     * (non-Javadoc)
     * 
     * @see eu.stratosphere.pact.common.stubs.ReduceStub#combine(java.util.Iterator,
     * eu.stratosphere.pact.common.stubs.Collector)
     */
    @Override
    public void combine(Iterator<PactRecord> records, Collector<PactRecord> out)
            throws Exception {

      // the logic is the same as in the reduce function, so simply call the reduce method
      this.reduce(records, out);
    }
  }

  /**
   * {@inheritDoc}
   */
//  @Override
  public Plan getPlan(String... args) {

    // parse job parameters
    int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output = (args.length > 2 ? args[2] : "");
    // index for field that will be counted
    int countFieldInd = (args.length > 3 ? Integer.parseInt(args[0]) : 0);

    FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
    MapContract mapper = MapContract.builder(QueryLogMapper.class).input(source).name("Read field from Query logs").build();
    // set field for counting
    mapper.setParameter(QueryLogMapper.COUNT_FIELD, countFieldInd);

    ReduceContract reducer = new ReduceContract.Builder(CountKeys.class, PactString.class, 0).input(mapper).name("Count Field Contents").build();
    FileDataSink out = new FileDataSink(CountOutFormat.class, output, reducer, "Field Content Counts");
    // TODO really?
    // source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII"); // comment out this line for
    // UTF-8
    // inputs

    Plan plan = new Plan(out, "Query Log Field Content Counter");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }

  /**
   * {@inheritDoc}
   */
//  @Override
  public String getDescription() {

    return "Parameters: [noSubStasks] [input] [output]";
  }

}
