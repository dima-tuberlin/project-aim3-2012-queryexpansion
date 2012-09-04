package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class CoOccRefReducer extends ReduceStub {

	private final PactRecord outputRecord = new PactRecord();

	private final PactString query = new PactString();

	private final PactString ref = new PactString();

	private final PactString coOccRefValue = new PactString();

	private final PactInteger oneCount = new PactInteger(1);

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		// save all refinements for this session in a list
		List<String> sessionRefs = new ArrayList<String>();

		// get first entry to save field in output record
		PactRecord element = records.next();
		query.setValue(element.getField(3, PactString.class));
		// for all output query, ref and count are the same
		outputRecord.setField(3, query);
		outputRecord.setField(6, oneCount);
		// add first ref to list
		sessionRefs.add(element.getField(4, PactString.class).getValue());

		while (records.hasNext()) {
			// get ref field and add to list
			sessionRefs.add(records.next().getField(4, PactString.class)
					.getValue());
		}
		
		// emit all co occurrences with count one if there are more than 1 ref
		// in this session
		String firstRef;
		while (sessionRefs.size() > 1) {
			// get first ref and remove from list
			firstRef = sessionRefs.get(0);
			sessionRefs.remove(0);
			ref.setValue(firstRef);
			outputRecord.setField(4, ref);
			// make all co occ ref entries and emit them
			for (String coOccRef : sessionRefs) {
				coOccRefValue.setValue(coOccRef);
				outputRecord.setField(7, coOccRefValue);
				out.collect(outputRecord);
			}
		}
	}

}
