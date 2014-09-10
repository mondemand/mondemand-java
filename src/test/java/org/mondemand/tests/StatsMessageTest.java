package org.mondemand.tests;

import static org.junit.Assert.assertEquals;

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
      for(int j=0; j<100; ++j) {
        String key = "Key_" + rnd.nextInt();
        StatsMessage msg = new StatsMessage(key, typesToTest[i]);
        assertEquals(key, msg.getKey());
        assertEquals(typesToTest[i], msg.getType());
        assertEquals(0, msg.getCounter());

        // start adding stuff to the stat
        long total = 0;
        for(int cnt=0; cnt<1000; cnt++) {
          int nextVal = rnd.nextInt();
          msg.incrementBy(nextVal);
          total += nextVal;
        }
        // check the values again
        assertEquals(key, msg.getKey());
        assertEquals(typesToTest[i], msg.getType());
        assertEquals(total, msg.getCounter());
        String[] strVal = msg.toString().split(" : ");
        assertEquals(typesToTest[i].toString(), strVal[0]);
        assertEquals(key, strVal[1]);
        assertEquals(total + "", strVal[2]);

        // set methods
        String newKey = "Key_" + rnd.nextInt();
        StatType newType = (rnd.nextInt(2) == 0 ? StatType.Counter : StatType.Gauge);
        int newValue = rnd.nextInt();
        msg.setKey(newKey);
        msg.setType(newType);
        msg.setCounter(newValue);
        assertEquals(newKey, msg.getKey());
        assertEquals(newType, msg.getType());
        assertEquals(newValue, msg.getCounter());
      }
    }
  }
}
