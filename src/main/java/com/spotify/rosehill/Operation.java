package com.spotify.rosehill;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.MemcacheClient;

/**
* Created by noa on 01/03/15.
*/
public interface Operation {
  /**
   * Perform the provided operation at a sustained rate.
   *
   * @param client the client to perform the operation on
   * @param serial a value used to derive a key
   * @return the run time of the operation
   */
  public ListenableFuture<?> perform(MemcacheClient<String> client, int serial);
}
