package de.tu_berlin.dima.aim3.querysuggestion;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class Session {
  
  private String query;
  private long time;
  private List<String> docs;
  private List<String> refinements;

  public Session(String query, long time) {

    this.query = query;
    this.time = time;
    this.refinements= new ArrayList<String>();
    this.docs = new ArrayList<String>();
  }
  
  
  public void addSessionEntry(String refinement,String doc){
    refinements.add(refinement);
    docs.add(doc);
  }
  
   
  
  public String getQuery() {
  
    return query;
  }
  
  public void setQuery(String query) {
  
    this.query = query;
  }
  

  
  
  public long getTime() {
  
    return time;
  }


  
  public void setTime(long time) {
  
    this.time = time;
  }


  
  public List<String> getDocs() {
  
    return docs;
  }


  
  public List<String> getRefinements() {
  
    return refinements;
  }



  
}
