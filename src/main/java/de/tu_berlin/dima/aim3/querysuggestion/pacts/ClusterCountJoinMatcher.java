package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;


public class ClusterCountJoinMatcher extends MatchStub {

  private final PactInteger refCount = new PactInteger();

  /**
   * value1 one from clustering
   * value2 hold ref count
   * 
   * take ref count from value2 and set as count of value1 
   */
  @Override
  public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out)
          throws Exception {

      // set count field in record from clustering
      refCount.setValue(value2.getField(6, PactInteger.class).getValue());
      value1.setField(6, refCount);
      out.collect(value1);
    
  }

}
