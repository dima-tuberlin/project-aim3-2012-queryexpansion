package de.tu_berlin.dima.aim3.querysuggestion.pacts;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class TopCountReducer extends ReduceStub {
	
	public static final String MAX_TOP_COUNT= "maxTopCount";
	
	/** number of top entries emitted*/
	private int maxTopCount;

	/**
	 * 
	 * 
	 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void open(Configuration parameters) {
		this.maxTopCount = parameters.getInteger(MAX_TOP_COUNT, 10);
	}
	
	
	/**
	 * Find top n counts in records
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {		
		
		// buffer for sorting records by time
		PriorityQueue<PactRecord> topRefRecords = new PriorityQueue<PactRecord>(10,
				 new Comparator<PactRecord>() {
		  //TODO why error?
					@Override
					public int compare(PactRecord record1, PactRecord record2) {

						// get time values from records
						int count1 = record1.getField(6, PactInteger.class)
								.getValue();
						int count2 = record2.getField(6, PactInteger.class)
								.getValue();
						// sort ascending
						if (count1 < count2) {
							return -1;
						}
						if (count1 > count2) {

							return 1;
						}
						return 0;
					}
				});
		

	    // buffer values sorted by ref count and only keep top n
	    while (records.hasNext()) {
	      
	      // copy everything!
	      PactRecord copy = records.next().createCopy();
	      // add to que
	      topRefRecords.add(copy);
	      // remove smallest element if size over the max. top count
	      if (topRefRecords.size() > maxTopCount){
	      
	    	  topRefRecords.poll();

	      }
	    }
	    // emit top n
//	    System.out.println("Top Entries");
	    for (PactRecord topRec : topRefRecords){
	    	out.collect(topRec);
//	    	if (maxTopCount == 15){
//	    	  System.out.println("Top: " + maxTopCount +" for  " +  topRec.getField(3,PactString.class) + "\t| " + topRec.getField(4, PactString.class) +"\t| " + topRec.getField(5, PactString.class) + "\t"+ topRec.getField(6, PactInteger.class));
//	    	} else{
//	    	  System.out.println("Top: " + maxTopCount +" for  " +  topRec.getField(3,PactString.class) + "\t| " + topRec.getField(4, PactString.class) + "\t"+ topRec.getField(6, PactInteger.class));
//	    	}
	    }

	}

}
