/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.tdb.solver;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorConcat;
import org.apache.jena.atlas.iterator.SingletonIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.*;
import org.apache.jena.sparql.engine.main.*;
import org.apache.jena.tdb.TDBException;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.tdb.store.GraphTDB;
import org.apache.jena.tdb.store.NodeId;
import org.apache.jena.tdb.store.nodetable.NodeTable;
import org.apache.jena.tdb.store.nodetupletable.NodeTupleTable;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.jena.sparql.ARQConstants.symCachingUsed;
import static org.apache.jena.sparql.engine.main.solver.SolverLib.makeAbortable;

/**
 * Entry to the basic pattern solver for TDB1.
 * {@link SolverRX} is the single stage and include RDF-star.
 */
public class PatternMatchTDB1 {

    /** Non-reordering execution of a basic graph pattern, given an iterator of bindings as input */
    public static QueryIterator execute(GraphTDB graph, BasicPattern pattern,
                                        QueryIterator input, Predicate<Tuple<NodeId>> filter,
                                        ExecutionContext execCxt)
    {
        // Maybe default graph or named graph.
        NodeTupleTable ntt = graph.getNodeTupleTable();
        return execute(ntt, graph.getGraphName(), pattern, input, filter, execCxt);
    }

    /** Non-reordering execution of a quad pattern, given an iterator of bindings as input.
     *  GraphNode is Node.ANY for execution over the union of named graphs.
     *  GraphNode is null for execution over the real default graph.
     */
    public static QueryIterator execute(DatasetGraphTDB ds, Node graphNode, BasicPattern pattern,
                                        QueryIterator input, Predicate<Tuple<NodeId>> filter,
                                        ExecutionContext execCxt)
    {
        NodeTupleTable ntt = ds.chooseNodeTupleTable(graphNode);
        return execute(ntt, graphNode, pattern, input, filter, execCxt);
    }

    // The worker.  Callers choose the NodeTupleTable.
    //     graphNode may be Node.ANY, meaning we should make triples unique.
    //     graphNode may be null, meaning default graph

    private static QueryIterator execute(NodeTupleTable nodeTupleTable, Node graphNode, BasicPattern pattern,
                                         QueryIterator input, Predicate<Tuple<NodeId>> filter,
                                         ExecutionContext execCxt)
    {
        if ( Quad.isUnionGraph(graphNode) )
            graphNode = Node.ANY;
        if ( Quad.isDefaultGraph(graphNode) )
            graphNode = null;

        List<Triple> triples = pattern.getList();
        boolean anyGraph = (graphNode == null ? false : (Node.ANY.equals(graphNode)));

        int tupleLen = nodeTupleTable.getTupleTable().getTupleLen();
        if ( graphNode == null ) {
            if ( 3 != tupleLen )
                throw new TDBException("SolverLib: Null graph node but tuples are of length " + tupleLen);
        } else {
            if ( 4 != tupleLen )
                throw new TDBException("SolverLib: Graph node specified but tuples are of length " + tupleLen);
        }

        // Convert from a QueryIterator (Bindings of Var/Node) to BindingNodeId
        NodeTable nodeTable = nodeTupleTable.getNodeTable();
        Iterator<BindingNodeId> chain = Iter.map(input, SolverLibTDB.convFromBinding(nodeTable));
        List<Abortable> killList;

        CachingTriplesConnector cachingTriplesConnector = execCxt.getContext().get(ARQConstants.symCachingTriples);

        // Check if the join can be fully resolved on the cache
        if(cachingTriplesConnector != null && canJoinBeProcessedOnCache(nodeTupleTable, triples, execCxt) && filter == null){
            if(cachingTriplesConnector.useFaster()){
                if(!input.hasNext()){
                    return new QueryIterNullIterator(execCxt);
                }

                Binding selectionBinding = input.next();
                BindingNodeId selectionBN = SolverLibTDB.convert(selectionBinding, nodeTable);

                Iterator<BindingNodeId> chainSynthCache = Iter.of(selectionBN);
                Iterator<BindingNodeId> chainSynthJena = Iter.of(selectionBN);

                CacheCancellable cacheCancellable = new CacheCancellable();

                Iterator<BindingNodeId> cacheChain = processOnCache(
                  nodeTupleTable,
                  chainSynthCache,
                  triples,
                  execCxt,
                  cacheCancellable,
                  true);
                List<Abortable> killListCache = new ArrayList<>();
                cacheChain = makeAbortable(cacheChain, killListCache);


                List<Abortable> killListJena = new ArrayList<>();
                Iterator<BindingNodeId> internalChain = processInternal(triples, graphNode, chainSynthJena, nodeTupleTable, anyGraph, filter, execCxt, killListJena);
                ChainSelecter selectedChain = selectChain(cacheChain, internalChain, killListCache, killListJena, cacheCancellable);

                if(!selectedChain.hasNext()){
                    return new QueryIterNullIterator(execCxt);
                }

                if(selectedChain.status.selected == ChainSelecter.Selection.NOT_YET){
                    throw new RuntimeException("Expected selection by this point");
                }

                if(selectedChain.status.selected == ChainSelecter.Selection.JENA){
                    List<Abortable> killListJena2 = new ArrayList<>();
                    Iterator<BindingNodeId> jenaChainNext = processInternal(triples, graphNode, chain, nodeTupleTable, anyGraph, filter, execCxt, killListJena2);
                    if(jenaChainNext.hasNext() && selectedChain.hasNext()){
                        chain = IteratorConcat.concat(selectedChain, jenaChainNext);
                    }
                    else if(jenaChainNext.hasNext()){
                        chain = jenaChainNext;
                    }
                    else if(selectedChain.hasNext()){
                        chain = selectedChain;
                    }
                    else {
                        chain = Iter.empty();
                    }
                    killList = Stream.of(killListJena, killListJena2)
                      .flatMap(Collection::stream)
                      .collect(Collectors.toList());
                }
                else{
                    Iterator<BindingNodeId> cacheChainNext = processOnCache(
                      nodeTupleTable,
                      chain,
                      triples,
                      execCxt,
                      cacheCancellable,
                      false);
                    List<Abortable> killListCache2 = new ArrayList<>();
                    cacheChainNext = makeAbortable(cacheChainNext, killListCache2);
                    if(cacheChainNext.hasNext() && selectedChain.hasNext()){
                        chain = IteratorConcat.concat(selectedChain, cacheChainNext);
                    }
                    else if(cacheChainNext.hasNext()){
                        chain = cacheChainNext;
                    }
                    else if(selectedChain.hasNext()){
                        chain = selectedChain;
                    }
                    else {
                        chain = Iter.empty();
                    }
                    killList = Stream.of(killListCache, killListCache2)
                      .flatMap(Collection::stream)
                      .collect(Collectors.toList());
                }
            }
            else {
                Iterator<BindingNodeId> cacheChain = processOnCache(
                  nodeTupleTable, chain, triples, execCxt, null, false);
                List<Abortable> killListCache = new ArrayList<>();
                cacheChain = makeAbortable(cacheChain, killListCache);
                chain = cacheChain;
                killList = killListCache;
            }
        }
        else {
            killList = new ArrayList<>();
            chain = processInternal(triples, graphNode, chain, nodeTupleTable, anyGraph, filter, execCxt, killList);
        }

        Iterator<Binding> iterBinding = SolverLibTDB.convertToNodes(chain, nodeTable);

        // "input" will be closed by QueryIterAbortable but is otherwise unused.
        // "killList" will be aborted on timeout.
        return new QueryIterAbortable(iterBinding, killList, input, execCxt);
    }

    private static ChainSelecter selectChain(
      Iterator<BindingNodeId> cacheChain,
      Iterator<BindingNodeId> internalChain,
      List<Abortable> killListCache,
      List<Abortable> killListJena,
      CacheCancellable cacheCancellable
    ) {
        return new ChainSelecter(cacheChain, internalChain, killListCache, killListJena, cacheCancellable);
    }

    private static Iterator<BindingNodeId> processInternal(
      List<Triple> triples,
      Node graphNode,
      Iterator<BindingNodeId> chain,
      NodeTupleTable nodeTupleTable,
      boolean anyGraph ,
      Predicate<Tuple<NodeId>> filter,
      ExecutionContext execCxt,
      List<Abortable> killList) {
        for (Triple triple : triples) {
            Tuple<Node> patternTuple = null;
            if (graphNode == null)
                // 3-tuples
                patternTuple = TupleFactory.create3(triple.getSubject(), triple.getPredicate(), triple.getObject());
            else
                // 4-tuples.
                patternTuple = TupleFactory.create4(graphNode, triple.getSubject(), triple.getPredicate(), triple.getObject());
            // Plain RDF, no RDF-star
            // chain = solve(nodeTupleTable, tuple, anyGraph, chain, filter, execCxt)
            // ;
            // RDF-star SA
            chain = matchQuadPattern(chain, graphNode, triple, nodeTupleTable, patternTuple, anyGraph, filter, execCxt);

            chain = makeAbortable(chain, killList);
        }
        return chain;
    }

    private static Iterator<BindingNodeId> processOnCache(
      NodeTupleTable nodeTupleTable,
      Iterator<BindingNodeId> chain,
      List<Triple> triples,
      ExecutionContext execCxt,
      CacheCancellable cacheCancellable,
      boolean setFirstSmall) {
        CachingTriplesConnector cachingTriplesConnector = execCxt.getContext().get(ARQConstants.symCachingTriples);
        assert(cachingTriplesConnector != null);
        return Iter.flatMap(chain, bindingNodeId -> processOnCacheSingleBinding(
          nodeTupleTable,
          bindingNodeId,
          triples,
          cachingTriplesConnector,
          cacheCancellable,
          setFirstSmall
          ));
    }


    private static Iterator<BindingNodeId> processOnCacheSingleBinding(
      NodeTupleTable nodeTupleTable,
      BindingNodeId bindingNodeId,
      List<Triple> triples,
      CachingTriplesConnector cachingTriplesConnector,
      CacheCancellable cacheCancellable,
      boolean setFirstSmall) {
        List<Tuple<NodePatternExport>> bgp = new ArrayList<>(triples.size());
        for(Triple triple : triples){
            NodePatternExport subjectId = toNodePattern(nodeTupleTable, triple.getSubject(), bindingNodeId);
            NodePatternExport predicateId = toNodePattern(nodeTupleTable, triple.getPredicate(), bindingNodeId);
            NodePatternExport objectId = toNodePattern(nodeTupleTable, triple.getObject(), bindingNodeId);
            Tuple<NodePatternExport> pattern = TupleFactory.create3(subjectId, predicateId, objectId);
            bgp.add(pattern);
        }

        if(cacheCancellable.isCanceled()){
            System.out.println("cache cancelled, aborting");
            return Iter.empty();
        }

        BindingStreamTableBGP results = cachingTriplesConnector.accessDataBGP(bgp, cacheCancellable, setFirstSmall);

        return Iter.map(results.getResults(),
          joinSingleResult -> resultRowToBindingNodeId(joinSingleResult, results.getVariableNames()));
    }
    private static BindingNodeId resultRowToBindingNodeId(
      List<Long> results,
      List<String> variableNames
    ){
        assert(results.size() == variableNames.size());
        BindingNodeId bindingNodeId = new BindingNodeId();
        for(int i = 0; i < results.size(); i++){
            Long result = results.get(i);
            String varName = variableNames.get(i);
            bindingNodeId.put(Var.alloc(varName), NodeId.create(result));
        }
        return bindingNodeId;
    }

    private static NodePatternExport toNodePattern(
      NodeTupleTable nodeTupleTable,
      Node node,
      BindingNodeId bindingNodeId
    ){
        if(node.isVariable()){
            NodeId resolvedNode = bindingNodeId.get(Var.alloc(node));
            if(resolvedNode == null){ // binding didnt work
                return new NodePatternExport(node.getName());
            }
            return new NodePatternExport(resolvedNode.getId());
        }
        return new NodePatternExport(
          nodeTupleTable.getNodeTable().getNodeIdForNode(node).getId()
        );
    }

    private static boolean canJoinBeProcessedOnCache(
      NodeTupleTable nodeTupleTable,
      List<Triple> triples,
      ExecutionContext execCxt
    ){
        if(execCxt.getContext().get(symCachingUsed, false)){
            return false;
        }

        CachingTriplesConnector cachingTriplesConnector = execCxt.getContext().get(ARQConstants.symCachingTriples);
        if(cachingTriplesConnector == null){
            return false;
        }

        if (hasCartesianProduct(triples)) {
            return false;
        }

        execCxt.getContext().set(symCachingUsed, true);


        // Check for variables
        for(Triple triple : triples){
            if(triple.getPredicate().isVariable()){
                return false;
            }
        }

        // Transform predicates to nodeIds
        ArrayList<Long> predicatesNodeIds = new ArrayList<>(triples.size());
        for(Triple triple : triples){
            NodeId nodeId = nodeTupleTable.getNodeTable().getNodeIdForNode(triple.getPredicate());
            predicatesNodeIds.add(nodeId.getId());
        }

        return cachingTriplesConnector.canRetrieveListOfPredicates(predicatesNodeIds);
    }


    private static class DSUContainer {
        private int[] parents;
        Set<Integer> addedNodes = new HashSet<>();

        DSUContainer(int size) {
            parents = new int[size];
            for (int i = 0; i < size; i++) {
                parents[i] = i;
            }
        }

        public void union(int a, int b) {
            assert (a < parents.length && a >= 0);
            assert (b < parents.length && b >= 0);
            int p_a = find(a);
            int p_b = find(b);
            parents[p_b] = p_a;
            addedNodes.add(a);
            addedNodes.add(b);
        }

        public int find(int node) {
            assert (node < parents.length && node >= 0);
            int p = parents[node];
            if (p == node) {
                return node;
            }
            int res = find(p);
            parents[node] = res;
            return res;
        }

        public int countConnectedComponents() {
            Set<Integer> set = new HashSet<>();
            for (int i = 0; i < parents.length; i++) {
                if (!addedNodes.contains(i)) {
                    continue;
                }
                int p = find(i);
                set.add(p);
            }
            return set.size();
        }
    }

    private static void addVarCond(Node node, Map<String, List<Integer>> varsMap, int tripleId) {
        if (!node.isVariable()) {
            return;
        }
        String var = node.toString();
        if (!varsMap.containsKey(var)) {
            varsMap.put(var, new ArrayList<>());
        }
        varsMap.get(var).add(tripleId);
    }

    private static boolean hasCartesianProduct(List<Triple> triples) {
        Map<String, List<Integer>> varsMap = new HashMap<>();
        int tripleId = 0;
        for (Triple triple : triples) {
            addVarCond(triple.getSubject(), varsMap, tripleId);
            addVarCond(triple.getObject(), varsMap, tripleId);
            tripleId++;
        }
        DSUContainer dsuContainer = new DSUContainer(triples.size());
        varsMap.forEach((var, tripleIdsList) -> {
            for (int i = 0; i < tripleIdsList.size(); i++) {
                dsuContainer.union(tripleIdsList.get(i), tripleIdsList.get(0));
            }
        });
        return dsuContainer.countConnectedComponents() > 1;
    }

    private static Iterator<BindingNodeId> matchQuadPattern(Iterator<BindingNodeId> chain, Node graphNode, Triple tPattern,
                                                            NodeTupleTable nodeTupleTable, Tuple<Node> patternTuple, boolean anyGraph,
                                                            Predicate<Tuple<NodeId>> filter, ExecutionContext execCxt) {
        return SolverRX.matchQuadPattern(chain, graphNode, tPattern, nodeTupleTable, patternTuple, anyGraph, filter, execCxt);
    }
}
