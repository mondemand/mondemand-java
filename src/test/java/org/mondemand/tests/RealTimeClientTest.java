package org.mondemand.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mondemand.Context;
import org.mondemand.RealTimeClient;

public class RealTimeClientTest {
  RealTimeClient rtClient = RealTimeClient.getInstance("test", "127.0.0.1", 9000);
  Context context = new Context("key", "value");
  String key1 = "key1";
  String key2 = "key2";

  @Before
  public void setUp() throws Exception
  {
  }

  @After
  public void tearDown()
  {
  }

  @Test
  public void createClient()
  {
    System.setProperty("com.example", "abc");
    rtClient = RealTimeClient.getInstance("test", "127.0.0.1", 9000);
    assertEquals(rtClient.getClass(), RealTimeClient.class);
  }

  @Test
  public void testIncrement()
  {
    for (int i=0; i<1000; i++)
    {
      rtClient.increment(context, "key1");
      rtClient.increment(context, "key2", 100l);
    }
    assertTrue(rtClient.getMondemandContent().containsKey(context));
    assertTrue(rtClient.getMondemandContent().get(context).containsKey(key1));
    assertEquals(1000l, rtClient.getMondemandContent().get(context).get(key1).longValue());
    assertTrue(rtClient.getMondemandContent().containsKey(context));
    assertTrue(rtClient.getMondemandContent().get(context).containsKey(key2));
    assertEquals(100000l, rtClient.getMondemandContent().get(context).get(key2).longValue());
  }

  @Test
  public void testSingletonClient()
  {
    rtClient = RealTimeClient.getInstance("test", "127.0.0.1", 9000);
    assertEquals(100000l, rtClient.getMondemandContent().get(context).get(key2).longValue());
    assertEquals(1000l, rtClient.getMondemandContent().get(context).get(key1).longValue());
  }

  @Test
  public void testSingletonClientClear()
  {
    rtClient = RealTimeClient.getInstance("test", "127.0.0.1", 9000);
    rtClient.getMondemandContent().clear();
    assertTrue(rtClient.getMondemandContent().isEmpty());
  }

  @Test
  public void testMultiThreadIncrement() throws InterruptedException
  {
    for (int j = 0; j<100; j++)
    {
      System.out.println(j);
      class IncrementThread implements Runnable {
        public void run()
        {
          for (int i=0; i< 1000; i++)
          {
            rtClient.increment(context, key1);
          }
        }
      }

      Thread[] threads = new Thread[3];
      for (int n = 0; n<threads.length; n++)
      {
        Thread t = new Thread(new IncrementThread());
        t.start();
        threads[n] = t;
      }

      for(int i = 0; i < threads.length; i++)
      {
        threads[i].join();
      }

      Thread.sleep(100);
      System.out.println(rtClient.getMondemandContent());
      assertEquals(3000, rtClient.getMondemandContent().get(context).get(key1).longValue());
      Thread.sleep(10);
      rtClient.getMondemandContent().clear();
    }
  }

  @Test
  public void testFlush()
  {
    rtClient.startExports();
    System.out.println();
    try
    {
      Thread.sleep(60);
    }
    catch (InterruptedException ie)
    {
    }
    assertTrue(rtClient.getMondemandContent().isEmpty());
  }

}
