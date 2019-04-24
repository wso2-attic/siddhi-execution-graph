/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.graph;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Operator which is related to find the maximum clique size of a graph.
 */
@Extension(
        name = "maximumClique",
        namespace = "graph",
        description = "This extension returns the size of the maximum clique of a graph.",
        parameters = {
                @Parameter(
                        name = "main.vertex",
                        description = "This is the ID of the main vertex that is used to create a graph.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "refer.vertex",
                        description = "This is the ID of the 'refer vertex' that connects with the main vertex in the" +
                                "graph.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "notify.update",
                        description = "If this is set to `true` and if there is any update in the " +
                                "maximum clique of the graph, an alert is sent.",
                        type = {DataType.BOOL}),
        },
        returnAttributes = @ReturnAttribute(
                name = "sizeOfMaximumClick",
                description = "The size of the maximum clique of the graph.",
                type = {DataType.INT}),
        examples = @Example(
                syntax = "define stream CseEventStream (vertex1 String, vertex2 String); \n" +
                        "from CseEventStream#graph:maximumClique(vertex1,vertex2,false)  \n" +
                        "select maximumClique  \n" +
                        "insert all events into OutputStream;",
                description = "This query returns the maximum clique of a given graph.")

)

public class MaximumCliqueStreamProcessor extends StreamProcessor<State> {

    private VariableExpressionExecutor variableExpressionId;
    private VariableExpressionExecutor variableExpressionFriendId;
    private Graph graph = new Graph();
    private int maxClique = 0;
    private boolean notifyUpdates;
    private List<Attribute> attributeList = new ArrayList<Attribute>();

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 3) {
            throw new UnsupportedOperationException("Invalid no of arguments passed to " +
                    "graph:maximumClique," + "required 3, but found" +
                    attributeExpressionExecutors.length);
        } else {
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the first parameter " +
                        "of graph:maximumClique, Required a variable, but found a constant parameter  " +
                        attributeExpressionExecutors[0].getReturnType());
            } else {
                variableExpressionId = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            }
            if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the second parameter" +
                        " of graph:maximumClique, Required a variable, but found a constant parameter " +
                        attributeExpressionExecutors[1].getReturnType());
            } else {
                variableExpressionFriendId = (VariableExpressionExecutor) attributeExpressionExecutors[1];
            }
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
                    notifyUpdates = (Boolean) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new SiddhiAppValidationException("MaximumClique's third parameter " +
                            "attribute should be a boolean value, but found " + attributeExpressionExecutors[2].
                            getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("MaximumClique should have constant" +
                        " parameter attribute but found a dynamic attribute " + attributeExpressionExecutors[2].
                        getClass().getCanonicalName());
            }
        }
        attributeList.add(new Attribute("maximumClique", Attribute.Type.INT));
        return null;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                String id = (String) variableExpressionId.execute(event);
                String friendsId = (String) variableExpressionFriendId.execute(event);
                graph.addEdge(id, friendsId);
                int newMaxClique = getMaxCliqueSize(id, maxClique);
                if (maxClique != newMaxClique) {
                    maxClique = newMaxClique;
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newMaxClique});
                } else if (notifyUpdates) {
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newMaxClique});
                } else {
                    streamEventChunk.remove();
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    /**
     * This is called to prune the candidates and call the overloaded method
     *
     * @param vertexId             newly added vertex
     * @param currentMaxCliqueSize current maximum clique size
     * @return size of the maximum clique
     */
    private int getMaxCliqueSize(String vertexId, int currentMaxCliqueSize) {
        HashSet<String> clique = new HashSet<String>();
        clique.add(vertexId);
        Set<String> neighbors = graph.getNeighbors(vertexId);
        Set<String> copyNeighbors = new HashSet<String>();
        copyNeighbors.addAll(neighbors);
        Iterator<String> iterator = copyNeighbors.iterator();

        // Prune candidates: vertices with smaller degree
        // than current max clique size can never be in the max clique

        while (iterator.hasNext()) {
            String candidate = iterator.next();
            if (graph.getDegree(candidate) < currentMaxCliqueSize) {
                iterator.remove();
            }
        }
        int maxClique = getMaxCliqueSize(clique, copyNeighbors, currentMaxCliqueSize);
        return maxClique;
    }

    /**
     * This method is a recursive method which perform incremental clique algorithm to find largest clique
     *
     * @param clique               clique which belongs newly added vertex
     * @param candidates           neighbours of the newly added vertex
     * @param currentMaxCliqueSize current maximum clique size
     * @return size of the maximum clique
     */
    private int getMaxCliqueSize(Set<String> clique, Set<String> candidates, int currentMaxCliqueSize) {
        Iterator<String> iterator = candidates.iterator();
        // pruning: filter candidates _ all candidates must have at least local degree currentMaxCliqueSize
        // into the clique
        // vertices, otherwise they could never participate in a larger clique as currentMaxCliqueSize
        while (iterator.hasNext()) {
            String candidate = iterator.next();
            if (getLocalDegree(candidate, clique) < clique.size()) {
                iterator.remove();
            }
        }
        // pruning2: if clique size + candidates size smaller currentMaxCliqueSize, we can never achieve
        // currentMaxCliqueSize
        if (clique.size() + candidates.size() < currentMaxCliqueSize) {
            return currentMaxCliqueSize;
        }
        int maxCliqueSize = clique.size();
        for (String candidate : candidates) {
            if (isCliqueMember(candidate, clique)) {

                Set<String> newClique = new HashSet<String>();
                newClique.addAll(clique);
                newClique.add(candidate);

                Set<String> newCandidates = new HashSet<String>();
                newCandidates.addAll(candidates);
                newCandidates.remove(candidate);

                int tempMaxCliqueSize = getMaxCliqueSize(newClique, newCandidates, Math.max(currentMaxCliqueSize,
                        maxCliqueSize));
                if (tempMaxCliqueSize > maxCliqueSize) {
                    maxCliqueSize = tempMaxCliqueSize;
                }
            }
        }
        return maxCliqueSize;
    }

    /**
     * get the degree of the vertex
     *
     * @param userId vertex Id
     * @param clique clique which vertex belongs to
     * @return degree of the vertex
     */
    private int getLocalDegree(String userId, Set<String> clique) {
        int count = 0;
        for (String cliqueMember : clique) {
            if (graph.existsEdge(userId, cliqueMember)) {
                count++;
            }
        }
        return count;
    }

    /**
     * return true if the vertex exists in the clique
     *
     * @param candidate vertex which we are going to check the existence
     * @param clique    the clique in which we search the vertex
     * @return true or false
     */
    private boolean isCliqueMember(String candidate, Set<String> clique) {
        for (String cliqueVertex : clique) {
            if (!graph.existsEdge(candidate, cliqueVertex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * This will be called only once and this can be used to acquire required resources for the
     * processing element.
     * This will be called after initializing the system and before starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release the acquired resources
     * for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributeList;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }
}
