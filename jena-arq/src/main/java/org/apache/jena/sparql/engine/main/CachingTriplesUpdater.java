package org.apache.jena.sparql.engine.main;

import org.apache.jena.atlas.lib.tuple.Tuple;

public interface CachingTriplesUpdater {
    void addTriple(Tuple<byte[]> triple);

    void deleteTriple(Tuple<byte[]> triple);
}
