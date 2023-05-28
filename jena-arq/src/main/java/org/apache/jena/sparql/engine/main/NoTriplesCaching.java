package org.apache.jena.sparql.engine.main;

import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.lib.tuple.Tuple;

public class NoTriplesCaching implements CachingTriplesConnector {

	@Override
	public boolean canRetrieve(Tuple<Long> tPattern) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean canRetrieveListOfPredicates(List<Long> predicatesNodeIds) {
		return false;
	}

	@Override
	public Iterator<Tuple<Long>> accessData(Tuple<Long> tPattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BindingStreamTableBGP accessDataBGP(List<Tuple<NodePatternExport>> bgp) {
		return null;
	}

	@Override
	public boolean isCaching() {
		// TODO Auto-generated method stub
		return false;
	}

}
