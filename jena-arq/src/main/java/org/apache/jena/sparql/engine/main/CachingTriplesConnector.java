package org.apache.jena.sparql.engine.main;

import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.lib.tuple.Tuple;

public interface CachingTriplesConnector {

	boolean canRetrieve(Tuple<Long> tPattern);
	boolean canRetrieveListOfPredicates(List<Long> predicatesNodeIds);

	Iterator<Tuple<Long>> accessData(Tuple<Long> tPattern);
	BindingStreamTableBGP accessDataBGP(
		List<Tuple<NodePatternExport>> bgp,
		CacheCancellable cacheCancellable,
		boolean setFirstSmall);

	boolean isCaching();

	boolean useFaster();

}
