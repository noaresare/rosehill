package com.spotify.rosehill;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.spotify.folsom.ConnectFuture;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import com.spotify.folsom.MemcacheStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The main class
 */
public class RosehillMain {

  private static final Logger log = LoggerFactory.getLogger(RosehillMain.class);

  private MemcacheClient<String> memcacheClient;

  public RosehillMain(String memcacheHost) {
    memcacheClient = getClient(memcacheHost);
  }

  public static void main(String[] args) {

    RosehillMain main = new RosehillMain("trusty0");
    main.populate(1000 * 1000);
  }

  private void populate(int count) {
    final CountDownLatch latch = new CountDownLatch(count);
    final AtomicInteger outstanding = new AtomicInteger();
    for (int i = 0; i < count; i++) {
      if (i % 1000 == 0) {
        log.info("issuing write " + i);
      }
      outstanding.incrementAndGet();
      Futures.addCallback(memcacheClient.replace(Integer.toString(i), "foobar", 0),
          new FutureCallback<MemcacheStatus>() {
            @Override
            public void onSuccess(MemcacheStatus result) {
              if (result != MemcacheStatus.OK) {
                throw new RuntimeException("Store failed, server returned " + result);
              }
              log.debug("Got another response currently outstanding: " + outstanding.decrementAndGet());
              latch.countDown();
            }


            @Override
            public void onFailure(Throwable t) {
              latch.countDown();
              throw new RuntimeException("Store failed", t);
            }
          });
    }
    try {
      latch.await();
      log.info("latch is now counted down");

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    memcacheClient.shutdown();
  }

  private static MemcacheClient<String> getClient(final String memcacheHost) {

    MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient();
    builder.withAddress(memcacheHost);
    MemcacheClient<String> client =  builder.connectAscii();
    try {
      ConnectFuture.connectFuture(client).get(1, TimeUnit.SECONDS);
      log.info("Connected to " + memcacheHost);
    } catch (TimeoutException e) {
      throw new Error("Timeout, Failed to connect to memcache server after 5 seconds");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return client;
  }
}
