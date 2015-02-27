package com.spotify.rosehill;

import com.spotify.folsom.ConnectFuture;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The main class
 */
public class RosehillMain {

  private static final Logger log = LoggerFactory.getLogger(RosehillMain.class);

  private MemcacheClient<String> memcacheClient;

  public RosehillMain(String memcacheHost) {
    memcacheClient = getClient(memcacheHost);
  }

  public static void main(String[] args) throws Throwable {

    RosehillMain main = new RosehillMain("trusty0");
    main.populate(1000 * 1000);
  }


  private void populate(int count) throws Throwable {
    SustainableRateClient rateClient = new SustainableRateClient(memcacheClient, 900);
    rateClient.runSetCommands(count);
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
