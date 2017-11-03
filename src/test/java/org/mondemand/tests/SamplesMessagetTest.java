package org.mondemand.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.junit.Test;
import org.mondemand.SampleTrackType;
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
      assertEquals(sampleSize, msg.getUpdateCounts());
    }
  }

  @Test
  public void testGetStats()
  {
    {
      // Test all stats
      final int allStats = Arrays.stream(SampleTrackType.values()).mapToInt(s -> s.value).sum();
      SamplesMessage msg = new SamplesMessage("blah", allStats);
      for (int i = 100; i > 0; --i) {
        // Add samples backwards to test sorting
        msg.addSample(i * 10);
      }
      final long sum = IntStream.range(1, 101).sum() * 10;

      Map<SampleTrackType, Long> stats = msg.getStats();
      for (Map.Entry<SampleTrackType, Long> stat : stats.entrySet()) {
        switch (stat.getKey()) {
          case MIN:
            assertEquals(10, stat.getValue().longValue());
            break;
          case MAX:
            assertEquals(1000, stat.getValue().longValue());
            break;
          case AVG:
            assertEquals(sum / 100, stat.getValue().longValue());
            break;
          case MEDIAN:
            assertEquals(500, stat.getValue().longValue());
            break;
          case PCTL_75:
            assertEquals(750, stat.getValue().longValue());
            break;
          case PCTL_90:
            assertEquals(900, stat.getValue().longValue());
            break;
          case PCTL_95:
            assertEquals(950, stat.getValue().longValue());
            break;
          case PCTL_98:
            assertEquals(980, stat.getValue().longValue());
            break;
          case PCTL_99:
            assertEquals(990, stat.getValue().longValue());
            break;
          case SUM:
            assertEquals(sum, stat.getValue().longValue());
            break;
          case COUNT:
            assertEquals(100, stat.getValue().longValue());
            break;
        }
      }
    }
    {
      // Test No stats
      SamplesMessage msg = new SamplesMessage("blah", 0, 100);
      for (int i = 100; i > 0; --i) {
        msg.addSample(i * 10);
      }
      Map<SampleTrackType, Long> stats = msg.getStats();
      assertTrue(stats.isEmpty());
    }
    {
      // Test partial stats
      SamplesMessage msg = new SamplesMessage("blah", SampleTrackType.MEDIAN.value, 100);
      for (int i = 100; i > 0; --i) {
        msg.addSample(i * 10);
      }
      Map<SampleTrackType, Long> stats = msg.getStats();
      assertEquals(1, stats.size());
      assertEquals(500, stats.get(SampleTrackType.MEDIAN).longValue());
    }
  }

  public class TestUpdater implements Runnable {
    final SamplesMessage msg;
    public int count = 0;

    public TestUpdater(SamplesMessage msg) {
      this.msg = msg;
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        if (msg.addSample(ThreadLocalRandom.current().nextInt(1000))) {
          ++count;
        }
      }
    }
  }

  public class TestEmitter implements Runnable {
    final SamplesMessage msg;
    public int count = 0;

    public TestEmitter(SamplesMessage msg) {
      this.msg = msg;
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        System.out.println(msg.getStats().get(SampleTrackType.COUNT));
        ++count;
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          // interrupted
          return;
        }
      }
    }
  }

  /**
   * This doesn't really test anything, its just to measure performance changes
   * @throws InterruptedException
   */
  @Test
  public void testThroughput() throws InterruptedException {
    final int updaterCount = 100;

    System.out.format("---%d updaters, 1 emitter 100 ms sleep---:\n", updaterCount);
    SamplesMessage msg =
        new SamplesMessage("blah", SampleTrackType.MEDIAN.value + SampleTrackType.AVG.value + SampleTrackType.COUNT.value,
            SamplesMessage.MAX_SAMPLES_COUNT);
    List<TestUpdater> updaters = new ArrayList<>(updaterCount);
    for (int i = 0; i < updaterCount; ++i) {
      updaters.add(new TestUpdater(msg));
    }
    TestEmitter emitter = new TestEmitter(msg);

    List<Thread> threads = new ArrayList<>(updaterCount + 1);
    for (TestUpdater updater : updaters) {
      threads.add(new Thread(updater));
    }
    threads.add(new Thread(emitter));

    long startTime = System.nanoTime();
    for (Thread t : threads) {
      t.start();
    }
    Thread.sleep(10500);
    for (Thread t : threads) {
      t.interrupt();
    }
    for (Thread t : threads) {
      t.join(50);
    }
    long elapsedTime = System.nanoTime() - startTime;

    System.out.format("Time: %d\n", elapsedTime);
    System.out.format("Updates: %d\n", updaters.stream().mapToInt(u -> u.count).sum());
    System.out.format("Emits %d\n\n", emitter.count);
  }
}
