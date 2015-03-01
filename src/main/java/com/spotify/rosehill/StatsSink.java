package com.spotify.rosehill;

public interface StatsSink {

  /**
   * This method stores the provided duration for statistics purposes.
   *
   * @param msDuration the duration in milliseconds
   */
  public void submitStat(int msDuration);

}
