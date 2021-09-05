package org.apache.jena.sparql.engine.main;

import org.apache.jena.graph.Triple;

public interface CachingTriplesUpdater {
    void addTriple(Triple triple);

    void deleteTriple(Triple triple);
}
