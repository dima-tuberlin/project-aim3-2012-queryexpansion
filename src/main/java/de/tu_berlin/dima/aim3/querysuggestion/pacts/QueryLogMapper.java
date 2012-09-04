package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tu_berlin.dima.aim3.querysuggestion.TimeUtils;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

public class QueryLogMapper
        extends MapStub {

  private static final Log LOG = LogFactory.getLog(QueryLogMapper.class);

  private static final String EMPTY_QUERY = "-";

  private final PactRecord outputRecord = new PactRecord();

  private final PactInteger userId = new PactInteger();

  private final PactLong time = new PactLong();

  private final PactString query = new PactString();

  private final PactString doc = new PactString();

  @Override
  public void map(PactRecord record, Collector<PactRecord> out)
          throws Exception {

    // the query log line is the first field from the record of the
    // TextInputFormat
    String[] fields = record.getField(0, PactString.class).toString().split("\t");

    // debugging
    // LOG.info("inputline: " + record.getField(0, PactString.class).toString());
    // filter out empty queries
    if (!fields[1].equals(EMPTY_QUERY)) {
      // TODO solve problem of first line in a better way

      try {
        userId.setValue(Integer.parseInt(fields[0]));
        // exclude strange user with 
        if (userId.getValue() != 71845) {
          query.setValue(fields[1]);

          time.setValue(TimeUtils.parseTimeToLong(fields[2]));

          // try to get clicked document
          doc.setValue("");
          // String url = "";
          if (fields.length == 5) {
            doc.setValue(fields[4]);
          }
          outputRecord.setField(0, userId);
          outputRecord.setField(1, time);
          outputRecord.setField(3, query);
          outputRecord.setField(5, doc);

          // emit valid log entry
          out.collect(outputRecord);
        }
      } catch (Exception e) {
        LOG.error("could not parse line: ||" + record.getField(0, PactString.class).toString() + "||");// TODO:

        // exception
      }

      // System.out.println(userId + "\t" + time + "\t" + query + "\t" +
      // doc);
    }

  }

}
