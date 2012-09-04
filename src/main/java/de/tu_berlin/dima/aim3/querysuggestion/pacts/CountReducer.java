package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

@Combinable
public class CountReducer extends ReduceStub {

	// private final PactRecord outputRecord = new PactRecord();

	private final PactString query = new PactString();

	private final PactString ref = new PactString();

	private final PactInteger theInteger = new PactInteger();

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {

		PactRecord element = null;
		int sum = 0;
		while (records.hasNext()) {
			element = records.next();
			// get count
			PactInteger i = element.getField(6, PactInteger.class);
			sum += i.getValue();
		}

		// update count field
		this.theInteger.setValue(sum);

		// outputRecord.setField(0, new PactString(element.getField(0,
		// PactString.class).getValue()));
		element.setField(6, this.theInteger);
		// outputRecord.setField(2, new PactString(element.getField(2,
		// PactString.class).getValue()));
		// TODO check if correct way to delete field
		// delete sessionId
		element.setNull(2);
		// TODO delete doc only if parameter set
//		// delete doc
//		element.setNull(5);
		out.collect(element);
//		System.out.println(element.getField(3, PactString.class) + "\t| "
//				+ element.getField(4, PactString.class) + " :"
//				+ element.getField(6, PactInteger.class));

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.pact.common.stubs.ReduceStub#combine(java.util.Iterator,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void combine(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {

		// the logic is the same as in the reduce function, so simply call the
		// reduce method
		this.reduce(records, out);
	}
}
