package org.apache.jena.sparql.engine.main;

public class NodePatternExport {
  private Long nodeId;
  private String variableName;

  private boolean isConcrete;
  public NodePatternExport(
    Long nodeId
  ){
    this.nodeId = nodeId;
    this.isConcrete = true;
  }
  public NodePatternExport(
    String variableName
  ){
    this.variableName = variableName;
    this.isConcrete = false;
  }
  public boolean isVariable(){
    return !this.isConcrete;
  }
  public boolean isConcrete(){
    return this.isConcrete;
  }
  public String getVariableName(){
    return this.variableName;
  }
  public Long getNodeId(){
    return this.nodeId;
  }
}
