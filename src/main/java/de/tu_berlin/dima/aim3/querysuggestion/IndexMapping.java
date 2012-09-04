package de.tu_berlin.dima.aim3.querysuggestion;


import java.util.HashMap;
import java.util.Map;

public class IndexMapping {

	/** number of entries */
  private int count;
	
	private  Map<String, Integer> indexMap;
	private  Map<Integer,String> reverseMap;
//
//	static {
//		DICTIONARY = Maps.newHashMap();
//		DICTIONARY.put("one", 0);
//		DICTIONARY.put("ring", 1);
//		DICTIONARY.put("to", 2);
//		DICTIONARY.put("rule", 3);
//		DICTIONARY.put("them", 4);
//		DICTIONARY.put("all", 5);
//		DICTIONARY.put("find", 6);
//		DICTIONARY.put("bring", 7);
//		DICTIONARY.put("and", 8);
//		DICTIONARY.put("in", 9);
//		DICTIONARY.put("the", 10);
//		DICTIONARY.put("darkness", 11);
//		DICTIONARY.put("bind", 12);
//	}

	
	//TODO add trigger to have reverse mapping because it is only needed for refs not for docs
	// s. http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/index.html
	public IndexMapping() {
		 indexMap = new HashMap<String, Integer>();
		 reverseMap = new HashMap<Integer,String>();
		 count = 0;
	}

	public String stringOf(int index){
	  return reverseMap.get((Integer)index);
	}
	
	/** 
	 * Get index of a term
	 * 
	 * @param term
	 * @return
	 */
	public int indexOf(String term) {
		// check if term is already mapped to an index
		if (!indexMap.containsKey(term)){
			// add new mapping
			indexMap.put(term, count);
			reverseMap.put(count,term);
			count++;
			return count - 1;
		}
		return indexMap.get(term);
	}
	
	public int getIndexSize(){
		return indexMap.size();
	}

}
