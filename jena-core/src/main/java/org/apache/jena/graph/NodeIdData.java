package org.apache.jena.graph;

public class NodeIdData {
    String data;
    long value;
    public NodeIdData(String data, long value){
        this.data = data;
        this.value = value;
    }
    public String getData(){ return data; }
    public long getValue(){ return value; }
}