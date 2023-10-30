package org.apache.jena.sparql.engine.main;

import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.sparql.engine.iterator.IterAbortable;

import java.util.Iterator;
import java.util.List;

public class BindingStreamTableBGP {
  private Iterator<List<Long>> results;
  private List<String> variableNames;
  public BindingStreamTableBGP(
    Iterator<List<Long>> results,
    List<String> variableNames
  ){
    this.results = results;
    this.variableNames = variableNames;
  }
  public Iterator<List<Long>> getResults(){
    return this.results;
  }
  public List<String> getVariableNames(){
    return this.variableNames;
  }

}
