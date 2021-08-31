package org.apache.jena.sparql.engine.main;

import java.util.Iterator;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

public class NoTriplesCaching implements CachingTriplesConnector {

	@Override
	public boolean canRetrieve(Triple tPattern) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<Quad> accessData(Triple tPattern) {
		// TODO Auto-generated method stub
		return null;
	}

}
