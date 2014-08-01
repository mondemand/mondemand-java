package org.mondemand.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Random;

import org.junit.Test;
import org.mondemand.StatType;
import org.mondemand.StatsMessage;

public class StatsMessageTest {

  /**
   * this tests counter and gauge StatMessage types.
   */
  @Test
  public void testCounterGaugeStats() {
    Random rnd = new Random();
    StatType[] typesToTest = new StatType[]{StatType.Counter, StatType.Gauge};
    for(int i=0; i<typesToTest.length; i++) {
      String key = "Key_" + rnd.nextInt();
      int trackType = rnd.nextInt();
      StatsMessage msg = new StatsMessage(key, typesToTest[i], trackType);
      assertEquals(key, msg.getKey());
      assertEquals(typesToTest[i], msg.getType());
      assertEquals(0, msg.getCounter());
      assertEquals(0, msg.getTimerCounter());
      assertEquals(0, msg.getTimerUpdateCounts());
      assertEquals(0, msg.getTrackingTypeValue());
      assertNull(msg.getSamples());
      // start adding stuff to the stat
      long total = 0;
      for(int cnt=0; cnt<1000; cnt++) {
        int nextVal = rnd.nextInt();
        msg.incrementBy(nextVal);
        total += nextVal;
      }
      // check the values again
      assertEquals(total, msg.getCounter());
      // all timer related stats should be unaffected.
      assertEquals(0, msg.getTimerCounter());
      assertEquals(0, msg.getTimerUpdateCounts());
      assertEquals(0, msg.getTrackingTypeValue());
      assertNull(msg.getSamples());

      // set value
      int nextVal = rnd.nextInt();
      msg.setCounter(nextVal);
      assertEquals(nextVal, msg.getCounter());
    }
  }

  /**
   * this tests timer StatMessage type.
   */
  @Test
  public void testTimerStat() {
    Random rnd = new Random();
    String key = "Key_" + rnd.nextInt();

    for(int i=0; i<100; i++) {
      int trackType = rnd.nextInt() + 1;
      StatsMessage msg = new StatsMessage(key, StatType.Timer, trackType);
      assertEquals(key, msg.getKey());
      assertEquals(StatType.Timer, msg.getType());
      assertEquals(0, msg.getCounter());
      assertEquals(0, msg.getTimerCounter());
      assertEquals(0, msg.getTimerUpdateCounts());
      assertEquals(trackType, msg.getTrackingTypeValue());
      assertEquals(msg.getSamples().size(), 0);
      // increment counter, different sizes for samples
      int sampleSize = rnd.nextInt(1000) + 500;
      long total = 0;
      for(int cnt=0; cnt<sampleSize; cnt++) {
        int nextVal = rnd.nextInt();
        msg.incrementBy(nextVal);
        total += nextVal;
      }
      // check the values again
      assertEquals((long)total, (long)msg.getCounter());
      assertEquals(total, msg.getTimerCounter());
      assertEquals(sampleSize, msg.getTimerUpdateCounts());
      assertEquals(trackType, msg.getTrackingTypeValue());
      assertEquals(msg.getSamples().size(),
          (sampleSize > StatsMessage.MAX_SAMPLES_COUNT
              ? StatsMessage.MAX_SAMPLES_COUNT : sampleSize));
      // reset the samples and check the values
      msg.resetSamples();
      assertEquals(0, msg.getTimerCounter());
      assertEquals(0, msg.getTimerUpdateCounts());
      assertEquals(trackType, msg.getTrackingTypeValue());
      assertEquals(msg.getSamples().size(), 0);
    }
  }
}
