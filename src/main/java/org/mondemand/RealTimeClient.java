package org.mondemand;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.mondemand.transport.LWESTransport;

/**
 * This is an mondemand client that emit stats according to platformhash, site, aduit
 *
 */
public class RealTimeClient {
  private static final Logger LOG = Logger.getLogger(RealTimeClient.class);

  protected final String host;
  protected final int port;
  protected final Client client;
  protected final String clientName;
  private ConcurrentMap<Context, ConcurrentHashMap<String, AtomicLong>>  mondemandContent
        = new ConcurrentHashMap<Context, ConcurrentHashMap<String, AtomicLong>>();
  private volatile boolean enabled = true;
  protected final int INTERVAL = 60000;
  protected boolean statsReset = true;

  private static RealTimeClient rtClient = null;

  /**
   * constructor for the singleton
   * @param clientName
   * @param host
   * @param port
   */
  protected RealTimeClient(String clientName, String host, int port)
  {
    this.clientName = clientName;
    this.host = host;
    this.port = port;
    this.client = createClient();
  }

  /**
   * create a singleton for the realtime mondemand client
   * @param clientName
   * @param host
   * @param port
   * @return the singleton of the RealTimeMondemandClient
   */
  public static synchronized RealTimeClient getInstance(String clientName, String host, int port)
  {
    if (rtClient == null)
    {
      rtClient = new RealTimeClient(clientName, host, port);
    }
    return rtClient;
  }

  private Client createClient()
  {
    try
    {
      Client newClient = new Client(clientName);
      newClient.addTransport
      (new LWESTransport(InetAddress.getByName(host),
                         port,
                         null));
      String hostName = InetAddress.getLocalHost().getHostName();
      if (hostName.indexOf(".") > 0)
      {
        hostName = hostName.substring(0, hostName.indexOf("."));
      }

      newClient.addContext("host", hostName);

      LOG.info
      (String.format
       ("Created mondemand client (%s:%d, client=%s, host=%s)",
        host,
        port,
        clientName,
        hostName));

      return newClient;

    }
    catch (Exception e)
    {
      e.printStackTrace();
      LOG.error("System will shutdown due to error: ", e);
      System.exit(100);
    }

    return null;
  }


  /**
   * Increment the count for the map
   * @param ph : platformhash
   * @param site : siteId
   * @param adunit : adunitId
   * @param key : KeyType: blank, advertiser_revenue, etc
   * @param value : the value to increase
   */
  public void increment(Context context, String keyType, long value)
  {
    ConcurrentHashMap<String, AtomicLong> stats = mondemandContent.get(context);
    if (stats == null)
    {
      stats = new ConcurrentHashMap<String, AtomicLong>();
      stats.put(keyType, new AtomicLong(value));
    }
    else
    {
       if (!stats.containsKey(keyType))
      {
        stats.put(keyType, new AtomicLong(value));
      }
      else
      {
        stats.get(keyType).addAndGet(value);
      }
    }
    mondemandContent.put(context, stats);
  }

  /**
   * Increment the count for the map
   * @param ph : platformhash
   * @param site : siteId
   * @param adunit : adunitId
   * @param key : KeyType: blank, advertiser_revenue, etc
   */
  public void increment(Context context, String keyType )
  {
    increment(context, keyType, 1);
  }

  /**
   * emit the events
   */
  public void startExports()
  {
    Thread stats = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        while (enabled) {
          for (Map.Entry<Context, ConcurrentHashMap<String, AtomicLong>> entry : mondemandContent.entrySet())
          {
            entry.getKey().addContext(client);
            ConcurrentHashMap<String, AtomicLong> stats = entry.getValue();
            for (Map.Entry<String, AtomicLong> stat : stats.entrySet())
            {
              client.increment(stat.getKey(), stat.getValue().intValue());
            }
            client.flush(true);
          }
          mondemandContent.clear();
          try
          {
            Thread.sleep(INTERVAL);
          }
          catch (InterruptedException ie)
          {
            LOG.error("Interrupted while sleeping between mondemand events.", ie);
          }
        }
      }
    });
    stats.setDaemon(true);
    stats.start();
  }

  public ConcurrentMap<Context, ConcurrentHashMap<String, AtomicLong>> getMondemandContent() {
    return mondemandContent;
  }


}
