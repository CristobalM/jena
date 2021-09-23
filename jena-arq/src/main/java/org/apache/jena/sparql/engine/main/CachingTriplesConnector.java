package org.apache.jena.sparql.engine.main;

import java.util.Iterator;

import org.apache.jena.atlas.lib.tuple.Tuple;

public interface CachingTriplesConnector {

	boolean canRetrieve(Tuple<Long> tPattern);

	Iterator<Tuple<Long>> accessData(Tuple<Long> tPattern);
	
	boolean isCaching();

}
