package org.apache.jena.sparql.engine.main;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.sparql.modify.request.UpdateVisitor;

public interface CachingTriplesUpdater {
    void addTriple(Tuple<Long> triple);

    void deleteTriple(Tuple<Long> triple);

  UpdateVisitor createUpdateVisitorWrapper(UpdateVisitor prepareWorker);
}
