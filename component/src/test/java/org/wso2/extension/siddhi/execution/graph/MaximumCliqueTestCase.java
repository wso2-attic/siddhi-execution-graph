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
 * Test class for the MaximumCliqueStreamProcessor
 */
public class MaximumCliqueTestCase {
    private static final Logger log = Logger.getLogger(MaximumCliqueTestCase.class);
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
    public void maximumCliqueTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (vertex1 String, vertex2 String);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:maximumClique(vertex1,vertex2,false) " +
                "select maximumClique " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"11", "12"});
        inputHandler.send(new Object[]{"11", "15"});
        inputHandler.send(new Object[]{"12", "15"});
        inputHandler.send(new Object[]{"12", "13"});
        inputHandler.send(new Object[]{"12", "14"});
        inputHandler.send(new Object[]{"13", "15"});
        inputHandler.send(new Object[]{"13", "14"});
        inputHandler.send(new Object[]{"14", "15"});

        SiddhiTestHelper.waitForEvents(waitTime, 8, count, timeout);

        AssertJUnit.assertEquals(3, count.get());
        AssertJUnit.assertTrue(eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void maximumCliqueTest2() throws InterruptedException {

        log.info("MaximumCliqueTestCaseInvalidNoOfArguments");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 String, vertex2 String, modifyUpdate bool, parameter4 String);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:maximumClique(vertex1,vertex2,false,parameter4) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void maximumCliqueTest3() {

        log.info("MaximumCliqueTestCaseInvalidParameterTypeInFirstArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 bool, vertex2 String, modifyUpdate bool);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:maximumClique(true,vertex2,false) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void maximumCliqueTest4() {

        log.info("MaximumCliqueTestCaseInvalidParameterTypeInSecondArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 String, vertex2 bool, modifyUpdate bool);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:maximumClique(vertex1,true,false) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void maximumCliqueTest5() {

        log.info("MaximumCliqueTestCaseInvalidParameterTypeInThirdArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (vertex1 String, vertex2 String, modifyUpdate String);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:maximumClique(vertex1,vertex2,modifyUpdate) " +
                "select * " + "insert all events into outputStream ;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }
}
