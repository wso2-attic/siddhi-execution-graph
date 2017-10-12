/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Operator which is related to find the size of the largest connected component of a graph.
 */
@Extension(
        name = "sizeOfLargestConnectedComponent",
        namespace = "graph",
        description = "Returns the size of the largest connected component of a graph",
        parameters = {
                @Parameter(
                        name = "main.vertex",
                        description = "This is the ID of the main vertex that is used to create the graph",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "refer.vertex",
                        description = "This is the ID of the refer vertex that connects with the main vertex in the " +
                                "graph",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "notify.update",
                        description = "This will give an alert if there is any update in the largest connected" +
                                "component of the graph",
                        type = {DataType.BOOL}),
        },
        returnAttributes = @ReturnAttribute(
                name = "sizeOfLargestConnectedComponent",
                description = "Size of the largest connected component of a graph",
                type = {DataType.LONG}),
        examples = @Example(
        description = "Example for LargestConnectedComponent\n" +
                "This will return size of the largest connected component of a given graph.",
        syntax = "define stream cseEventStream (vertex1 String, vertex2 String); \n" +
                "from cseEventStream#graph:sizeOfLargestConnectedComponent(vertex1,vertex2,false) \n" +
                "select sizeOfLargestConnectedComponent \n" +
                "insert all events into outputStream ;")
)

public class LargestConnectedComponentProcessor extends StreamProcessor {

    private VariableExpressionExecutor variableExpressionId;
    private VariableExpressionExecutor variableExpressionFriendId;
    private Graph graph = new Graph();
    private long largestConnectedComponentSize = 0;
    private boolean notifyUpdates;

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or
     *                              modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                String vertexOneId = (String) variableExpressionId.execute(event);
                String vertexTwoId = (String) variableExpressionFriendId.execute(event);
                graph.addEdge(vertexOneId, vertexTwoId);
                long newLargestConnectedComponent = getLargestConnectedComponent();
                if (largestConnectedComponentSize != newLargestConnectedComponent) {
                    largestConnectedComponentSize = newLargestConnectedComponent;
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newLargestConnectedComponent});
                } else if (notifyUpdates) {
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newLargestConnectedComponent});
                } else {
                    streamEventChunk.remove();
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }


    /**
     * The init method of the LargestConnectedComponentProcessor,
     * this method will be called before other methods
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 explain here
     * @param siddhiAppContext              the context of the execution plan
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 3) {
            throw new UnsupportedOperationException("Invalid no of arguments passed to " +
                    "graph:sizeOfLargestConnectedComponent," + "required 3, but found" +
                    attributeExpressionExecutors.length);
        } else {
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the firs" +
                        "t parameter of graph:sizeOfLargestConnectedComponent, Required a variable," +
                        " but found a constant parameter  " + attributeExpressionExecutors[0].getReturnType());
            } else {
                variableExpressionId = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            }
            if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the second" +
                        " parameter of graph:sizeOfLargestConnectedComponent, Required a variable," +
                        " but found a constant parameter " + attributeExpressionExecutors[1].getReturnType());
            } else {
                variableExpressionFriendId = (VariableExpressionExecutor) attributeExpressionExecutors[1];
            }
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
                    notifyUpdates = (Boolean) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new SiddhiAppValidationException("sizeOfLargestConnectedComponent's" +
                            " third parameter attribute should be a boolean value, but found " +
                            attributeExpressionExecutors[2].getReturnType());
                }

            } else {
                throw new SiddhiAppValidationException("LargestConnectedComponent should have constant" +
                        " parameter attribute but found a dynamic attribute " + attributeExpressionExecutors[2].
                        getClass().getCanonicalName());
            }
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("size", Attribute.Type.LONG));
        return attributeList;
    }

    /**
     * Gets the number of vertices of the largest connected component of the graph
     *
     * @return the number of vertices in the largest connected component
     */
    private long getLargestConnectedComponent() {

        if (graph.size() == 0) {
            return 0L;
        }

        HashMap<String, Long> pegasusMap = new HashMap<String, Long>();

        long i = 0;
        for (String key : graph.getGraph().keySet()) {
            pegasusMap.put(key, i);
            i++;
        }
        boolean traversalPerformed;
        do {
            traversalPerformed = false;
            for (Map.Entry<String, Long> mainVertex : pegasusMap.entrySet()) {
                for (Map.Entry<String, Long> referVertex : pegasusMap.entrySet()) {
                    if (graph.existsEdge(mainVertex.getKey(), referVertex.getKey())) {

                        if (mainVertex.getValue() > referVertex.getValue()) {
                            pegasusMap.replace(mainVertex.getKey(), referVertex.getValue());
                            traversalPerformed = true;
                        } else if (referVertex.getValue() > mainVertex.getValue()) {
                            pegasusMap.replace(referVertex.getKey(), mainVertex.getValue());
                            traversalPerformed = true;
                        }
                    }
                }
            }
        } while (traversalPerformed);
        return calculateLargestComponent(pegasusMap);
    }

    /**
     * Calculates the size of largest connected component
     *
     * @param pegasusMap is the reference to the pegasusmap
     * @return size of largest connected component
     */
    private long calculateLargestComponent(HashMap<String, Long> pegasusMap) {
        long largestComponent = 0;
        for (Long pegasusValue : pegasusMap.values()) {
            int count = 0;
            for (Long referPegasusValue : pegasusMap.values()) {
                if (pegasusValue.equals(referPegasusValue)) {
                    count++;
                }
            }
            if (count > largestComponent) {
                largestComponent = count;
            }
        }
        return largestComponent;
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
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }

}
