package com.spotify.rosehill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



public class ArrayBackedStatsSink implements StatsSink {

  private static final Logger log = LoggerFactory.getLogger(ArrayBackedStatsSink.class);

  private final List<Integer> list;
  private double average = 0.0;
  int count = 0;


  public ArrayBackedStatsSink() {
    this.list = Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public void submitStat(int msDuration) {
    list.add(msDuration);
    count++;
    average += (msDuration - average) / count;
  }

  public void reset() {
    list.clear();
    count = 0;
    average = 0.0;
  }

  public void outputStats() {
    log.debug("Sorting stats...");
    Collections.sort(list);
    log.debug("done sorting list");
    log.info(String.format("Average latency: %.1f ", average));
    log.info("95th percentile latency: {}", list.get((int)(count * 0.95)));
    log.info("99th percentile latency: {}", list.get((int)(count * 0.99)));
    log.info("Highest latency: {} ", list.get(count - 1));
  }
}
