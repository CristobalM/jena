package org.apache.jena.sparql.engine.main;

import java.util.Iterator;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

public interface CachingTriplesConnector {

	boolean canRetrieve(Triple tPattern);

	Iterator<Quad> accessData(Triple tPattern);

}
