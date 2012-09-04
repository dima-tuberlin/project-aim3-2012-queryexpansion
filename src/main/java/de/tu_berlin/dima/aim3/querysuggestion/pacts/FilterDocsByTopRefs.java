package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class FilterDocsByTopRefs
        extends MatchStub {

  public static final String DEL_FIELD = "parameter.DEL_FIELD";

  /**
   * Reads the filter literals from the configuration.
   * 
   * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
   */
  @Override
  public void open(Configuration parameters) {

    // this.delField= parameters.getInteger(this.DEL_FIELD, 0);
  }

  @Override
  public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out)
          throws Exception {

    // TODO this has no impact yet
    // filter session entries without doc
    if (!value1.getField(5, PactString.class).getValue().equals("")) {

      // delete unwanted entry sessionID
      value1.setNull(2);

      out.collect(value1);
      System.out.println("Doc MATCH q: " + value1.getField(3, PactString.class) + "\tr: "
              + value1.getField(4, PactString.class) + "\td: |" + value1.getField(5, PactString.class) + "|\t"
              + value1.getField(6, PactInteger.class));

    }

  }

}
