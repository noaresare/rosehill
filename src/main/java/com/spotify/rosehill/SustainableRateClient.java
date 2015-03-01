package com.spotify.rosehill;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Instances of this class executes memcached commands at a sustained rate.
 */
public class SustainableRateClient {


  private static final Logger log = LoggerFactory.getLogger(RosehillMain.class);

  private final MemcacheClient<String> client;
  private final int maxOutstanding;
  private final StatsSink statsSink;

  private volatile Throwable throwable;
  private AtomicInteger outstanding = new AtomicInteger();

  public SustainableRateClient(MemcacheClient<String> client, int maxOutstanding,
                               StatsSink statsSink) {
    this.client = client;
    this.maxOutstanding = maxOutstanding;
    this.statsSink = statsSink;
  }

  public void run(final int total, Operation operation) throws Throwable {
    int count = 1;
    int tempOutstanding = outstanding.get();
    int target;
    int toSleep = 100;
    while (true) {
      target = count + maxOutstanding - tempOutstanding;
      while (count < target) {
        if (count == total) {
          log.debug("wrote {} records, exiting.", count);
          return;
        }
        doOperation(operation, count++);
      }
      Thread.sleep(toSleep);
      tempOutstanding = outstanding.get();
      if (tempOutstanding == 0) {
        toSleep = (int)(toSleep * 0.8);
        log.info("Outstanding is empty, decreasing sleep delay to {}", toSleep);
      }
      if (tempOutstanding == maxOutstanding * 0.9) {
        toSleep = (int)(toSleep * 1.2);
        log.debug("Outstanding is close to full, increasing sleep delay to {} ", toSleep);
      }
    }

  }

  private void doOperation(Operation operation, int i) throws Throwable {
    //log.debug("calling doOperation()");
    if (throwable != null) {
      throw throwable;
    }
    if (i % 1000 == 0) {
      log.debug("issuing " + i);
    }
    outstanding.incrementAndGet();
    final long startTime = System.currentTimeMillis();
    Futures.addCallback(operation.perform(client, i),
        new FutureCallback<Object>() {
          @Override
          public void onSuccess(Object result) {
            //log.info("this call took {} milliseconds", System.currentTimeMillis() - startTime);
            statsSink.submitStat((int)(System.currentTimeMillis() - startTime));
            outstanding.decrementAndGet();
            if (result instanceof MemcacheStatus && result != MemcacheStatus.OK) {
              throw new RuntimeException("Store failed, server returned " + result);
            }
          }


          @Override
          @ParametersAreNonnullByDefault
          public void onFailure(Throwable t) {
            log.info("this call took {} milliseconds", System.currentTimeMillis() - startTime);
            outstanding.decrementAndGet();
            throwable = t;
          }

        });
  }


}
