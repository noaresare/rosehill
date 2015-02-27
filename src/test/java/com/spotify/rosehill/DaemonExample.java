package com.spotify.rosehill;

/**
 * Created by noa on 27/02/15.
 */
public class DaemonExample {
  public static void main(String[] args) {
    Thread t = new Thread(new Runnable(){
      @Override
      public void run()  {
        try {
          for (int i = 0; i < 4; i++) {
            log("sleeping for a second");
            Thread.sleep(1000);
          }
        } catch (InterruptedException e) {
          throw new Error(e);
        }
      }
    });
    t.setDaemon(true);
    t.start();
    log("started thread, now exiting main");
  }

  private static void log(String s) {
    System.out.println(s);
  }
}
