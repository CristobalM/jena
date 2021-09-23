package org.apache.jena.sparql.engine.main;

import java.util.Iterator;

import org.apache.jena.atlas.lib.tuple.Tuple;

public class NoTriplesCaching implements CachingTriplesConnector {

	@Override
	public boolean canRetrieve(Tuple<Long> tPattern) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<Tuple<Long>> accessData(Tuple<Long> tPattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCaching() {
		// TODO Auto-generated method stub
		return false;
	}

}
