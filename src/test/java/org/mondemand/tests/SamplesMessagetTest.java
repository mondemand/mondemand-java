package org.mondemand.tests;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;
import org.mondemand.SamplesMessage;
import org.mondemand.StatType;

public class SamplesMessagetTest {

  /**
   * this tests SamplesMessage type.
   */
  @Test
  public void testSamples() {
    Random rnd = new Random();

    for(int i=0; i<100; i++) {
      String key = "Key_" + rnd.nextInt();
      int trackType = rnd.nextInt();

      int samplesMaxCount = SamplesMessage.MAX_SAMPLES_COUNT;
      if(rnd.nextInt(2) == 1) {
        samplesMaxCount += rnd.nextInt(500);
      } else {
        samplesMaxCount -= rnd.nextInt(500);
      }
      SamplesMessage msg;
      if(rnd.nextInt(2) == 0) {
        msg = new SamplesMessage(key, trackType, samplesMaxCount);
      } else {
        msg = new SamplesMessage(key, trackType);
        samplesMaxCount = SamplesMessage.MAX_SAMPLES_COUNT;
      }

      assertEquals(key, msg.getKey());
      assertEquals(StatType.Gauge, msg.getType());
      assertEquals(trackType, msg.getTrackingTypeValue());
      assertEquals(0, msg.getCounter());
      assertEquals(0, msg.getUpdateCounts());
      assertEquals(samplesMaxCount, msg.getSamplesMaxCount());
      assertEquals(0, msg.getSamples().size());

      // now add samples, different sizes for samples
      int sampleSize = rnd.nextInt(1000) + 500;
      long total = 0;
      for(int cnt=0; cnt<sampleSize; cnt++) {
        int nextVal = rnd.nextInt();
        msg.addSample(nextVal);
        total += nextVal;
      }
      // check the values again
      assertEquals(key, msg.getKey());
      assertEquals(StatType.Gauge, msg.getType());
      assertEquals(trackType, msg.getTrackingTypeValue());
      assertEquals(total, msg.getCounter());
      assertEquals(Math.min(samplesMaxCount, sampleSize), msg.getSamples().size());
      assertEquals(sampleSize, msg.getUpdateCounts());

      // reset the samples and check the values
      msg.resetSamples();
      assertEquals(key, msg.getKey());
      assertEquals(StatType.Gauge, msg.getType());
      assertEquals(trackType, msg.getTrackingTypeValue());
      assertEquals(0, msg.getCounter());
      assertEquals(0, msg.getUpdateCounts());
      assertEquals(samplesMaxCount, msg.getSamplesMaxCount());
      assertEquals(0, msg.getSamples().size());
    }
  }

}
