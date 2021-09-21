package org.apache.jena.sparql.engine.main;

import java.util.Iterator;

import org.apache.jena.atlas.lib.tuple.Tuple;

public interface CachingTriplesConnector {

	boolean canRetrieve(Tuple<byte[]> tPattern);

	Iterator<Tuple<byte[]>> accessData(Tuple<byte[]> tPattern);
	
	boolean isCaching();

}
