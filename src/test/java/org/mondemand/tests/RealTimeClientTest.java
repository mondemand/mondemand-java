package org.mondemand.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mondemand.Context;
import org.mondemand.RealTimeClient;

public class RealTimeClientTest {
  RealTimeClient rtClient = new RealTimeClient("test", "127.0.0.1", 9000);
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
    assertEquals(1000l, rtClient.getMondemandContent().get(context).get(key1));
    assertTrue(rtClient.getMondemandContent().containsKey(context));
    assertTrue(rtClient.getMondemandContent().get(context).containsKey(key2));
    assertEquals(100000l, rtClient.getMondemandContent().get(context).get(key2));
  }

  @Test
  public void testClientClear()
  {
    rtClient.getMondemandContent().clear();
    assertTrue(rtClient.getMondemandContent().isEmpty());
  }

  @Test
  public void testMultiThreadIncrement() throws InterruptedException
  {

    for (int j=0; j<100; j++)
    {
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

      assertEquals(3000, rtClient.getMondemandContent().get(context).get(key1));
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
