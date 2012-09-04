package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class FilterByTopRefMatcher
        extends MatchStub {

  public static final String DEL_FIELD = "parameter.DEL_FIELD";

  /** field of the record set to null before emitting */
  private int delField;

  /**
   * Reads the filter literals from the configuration.
   * 
   * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
   */
  @Override
  public void open(Configuration parameters) {

    // default is never used field
    this.delField = parameters.getInteger(this.DEL_FIELD, 100);
  }

  @Override
  public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out)
          throws Exception {

    // TODO really???:output only if query is not equal ref
    // if (value1.getFieldInto(3, target))

    // delete unwanted entry should be sessionID 2 or document 5
    value1.setNull(delField);

    // in doc mode only emit if doc field is not empty
    if (delField == 2) {
      if (!value1.getField(5, PactString.class).getValue().equals("")) {
        out.collect(value1);
//        System.out.println("Use this top ref doc entry for  " + value1.getField(3, PactString.class) + "\t| "
//                + value1.getField(4, PactString.class) + "\t" + value1.getField(5, PactString.class) + "\t"
//                + value1.getField(6, PactInteger.class) + "\t del: " + delField);
      }
    } else {
      out.collect(value1);
//      System.out.println("Use this top ref entry for  " + value1.getField(3, PactString.class) + "\t| "
//              + value1.getField(4, PactString.class) + "\t" + value1.getField(6, PactInteger.class) + "\t del: "
//              + delField);

    }

  }

}
