package org.jboss.examples.camel;

import java.util.ArrayList;
import java.util.List;
import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.camel.processor.aggregate.jdbc.OptimisticLockingJdbcAggregationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrayListAggregationStrategy implements AggregationStrategy {

  private static final Logger log = LoggerFactory.getLogger(ArrayListAggregationStrategy.class);

  @Override
  public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
    log.info(String.format("Current list: [%s]", (oldExchange != null) ? oldExchange.getIn().getBody() : null));
    log.info(String.format("Adding item: [%s]", newExchange.getIn().getBody()));
    log.info(String.format("Version: [%s]", (oldExchange != null) ? oldExchange.getProperty(OptimisticLockingJdbcAggregationRepository.VERSION_EXCHANGE_PROPERTY) : null));

    Exchange result = (oldExchange != null) ? oldExchange : newExchange;
    
    boolean firstrun = (oldExchange == null);
    List<Long> bodyList = (firstrun) ? new ArrayList<>() : oldExchange.getIn().getBody(List.class);
    bodyList.add(newExchange.getIn().getBody(Long.class));
    result.getIn().setBody(bodyList);
    
    /*
    This is only here to cause a conflict on adding to the aggregation 
    repository. If we don't sleep for a bit, things happen too fast and we 
    don't get the conflicts that we would get in the real world.
    */
    try { Thread.sleep(1000L); } catch (InterruptedException e) { }
    
    return result;
  }
}
