package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class SingleRefSesReducer extends ReduceStub{

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		// emit only first entry to make sure every ref is counted only once per session
		out.collect(records.next());		
	}

}
