package org.apache.jena.sparql.engine.main;

import org.apache.jena.atlas.lib.tuple.Tuple;

public interface CachingTriplesUpdater {
    void addTriple(Tuple<Long> triple);

    void deleteTriple(Tuple<Long> triple);
}
