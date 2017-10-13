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

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test class for the LargestConnectedComponentProcessor
 */
public class LargestConnectedComponentTestCase {
    private static final Logger log = Logger.getLogger(LargestConnectedComponentTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 2000;

    @BeforeClass
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void largestConnectedComponentTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 String, vertex2 String);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:sizeOfLargestConnectedComponent(vertex1,vertex2,false) " +
                "select * " + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"1234", "2345"});
        inputHandler.send(new Object[]{"2345", "5678"});
        inputHandler.send(new Object[]{"5678", "1234"});
        inputHandler.send(new Object[]{"5522", "3322"});
        inputHandler.send(new Object[]{"3322", "4567"});
        inputHandler.send(new Object[]{"4567", "7890"});
        inputHandler.send(new Object[]{"7890", "5428"});

        SiddhiTestHelper.waitForEvents(waitTime, 7, count, timeout);

        AssertJUnit.assertEquals(4, count.get());
        AssertJUnit.assertTrue(eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void largestConnectedComponentTest2() throws InterruptedException {

        log.info("LargestConnectedComponentTestCaseInvalidNoOfArguments");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 String, vertex2 String, modifyUpdate bool, parameter4 String);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:sizeOfLargestConnectedComponent(vertex1,vertex2,false,parameter4) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void largestConnectedComponentTest3() throws InterruptedException {

        log.info("LargestConnectedComponentTestCaseInvalidParameterTypeInFirstArgument");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 bool, vertex2 String, modifyUpdate bool);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:sizeOfLargestConnectedComponent(true,vertex2,false) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void largestConnectedComponentTest4() throws InterruptedException {

        log.info("LargestConnectedComponentTestCaseInvalidParameterTypeInSecondArgument");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 String, vertex2 bool, modifyUpdate bool);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:sizeOfLargestConnectedComponent(vertex1,true,false) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void largestConnectedComponentTest5() throws InterruptedException {

        log.info("LargestConnectedComponentTestCaseInvalidParameterTypeInThirdArgument");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 String, vertex2 String, modifyUpdate String);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:sizeOfLargestConnectedComponent(vertex1,vertex2,modifyUpdate) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }


}
