/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.examples.camel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.test.spring.CamelSpringJUnit4ClassRunner;
import org.apache.camel.test.spring.DisableJmx;
import org.apache.camel.test.spring.UseAdviceWith;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@RunWith(CamelSpringJUnit4ClassRunner.class)
@ContextConfiguration({"/META-INF/spring/testApplicationContext.xml",
                       "/META-INF/spring/applicationContext.xml"})
@DisableJmx(true)
@UseAdviceWith(true)
public class MessageResequencerTest {

  private static final Logger log = LoggerFactory.getLogger(MessageResequencerTest.class);

  @Autowired
  private CamelContext camelContext;

  @Produce(uri = "activemq:queue:org.jboss.examples.UnorderedMessages")
  private ProducerTemplate activemq;

  @EndpointInject(uri = "mock:org.jboss.examples.OrderedMessages")
  private MockEndpoint mock;

  @Before
  public void adviceResequencerRoute() throws Exception {
    camelContext.getRouteDefinition("resequencerRoute").adviceWith(camelContext.adapt(ModelCamelContext.class), new AdviceWithRouteBuilder() {
      @Override
      public void configure() throws Exception {
        weaveById("processMessage")
                .after()
                .to(mock);
      }
    });
    camelContext.addRoutes(new RouteBuilder() {
      @Override
      public void configure() throws Exception {
        from("activemq:queue:org.jboss.examples.UnorderedMessages?acknowledgementModeName=CLIENT_ACKNOWLEDGE")
          .routeId("resequencerRouteDup")
          .aggregate(header("MyGroupingID"))
            .aggregationStrategyRef("arrayListAggregationStrategy")
            .aggregationRepositoryRef("optimisticLockingJdbcAggregationRepository")
            .completionSize(10)
            //.completionTimeout(15000L)
            .split(simple("${body}")).parallelProcessing(false)
            .resequence(simple("${body}"))
              .log("Processed message #${body}.")
              .to(mock);
      }
    });
    camelContext.start();
  }

  @Test(timeout = 30000L)
  public void testUnorderedMessages() throws Exception {
    List<Integer> unorderedMessages = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      unorderedMessages.add(i);
    }
    Collections.shuffle(unorderedMessages);
    for (Integer message : unorderedMessages) {
      log.info(String.format("Sending message #%s...", message));
      activemq.sendBodyAndHeader(message, "MyGroupingID", 1);
    }
    mock.expectedMessageCount(10);
    mock.expectsAscending().simple("${body}");
    mock.await(30, TimeUnit.SECONDS);
    mock.assertIsSatisfied();
  }
}
