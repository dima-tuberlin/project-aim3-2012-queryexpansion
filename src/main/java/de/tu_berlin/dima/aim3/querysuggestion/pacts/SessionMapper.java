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

public class SessionMapper
        extends MapStub {

  private static final Log LOG = LogFactory.getLog(SessionMapper.class);

  private final PactRecord outputRecord = new PactRecord();

  private final PactString query = new PactString();

  private final PactString sessionId = new PactString("default");

  private final PactString ref = new PactString();

  private final PactString doc = new PactString();

  private final PactInteger oneCount = new PactInteger(1);

  @Override
  public void map(PactRecord record, Collector<PactRecord> out)
          throws Exception {

    // this.buffer.setLength(0);
    // this.buffer.append(record.getField(2, PactString.class).getValue());
    // this.buffer.append('\t');
    // this.buffer.append(record.getField(3, PactString.class).toString());
    // this.buffer.append('\t');
    // this.buffer.append(record.getField(4, PactString.class).getValue());
    // this.buffer.append('\t');
    // this.buffer.append(record.getField(5, PactString.class).getValue());
    // this.buffer.append('\t');
    // this.buffer.append(record.getField(6, PactInteger.class).getValue());
    // this.buffer.append('\n');

    // the query log line is the first field from the record of the
    // TextInputFormat
    String[] fields = record.getField(0, PactString.class).toString().split("\t");

    sessionId.setValue(fields[0]);
    query.setValue(fields[1]);
    ref.setValue(fields[2]);
    doc.setValue(fields[3]);

    outputRecord.setField(2, sessionId);
    outputRecord.setField(3, query);
    outputRecord.setField(4, ref);
    outputRecord.setField(5, doc);
    outputRecord.setField(6, oneCount);

    // emit valid log entry
    out.collect(this.outputRecord);
  }

}
