package org.mondemand;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.mondemand.transport.LWESTransport;

import com.google.common.util.concurrent.AtomicLongMap;

/**
 * This is an mondemand client that emit stats according to different context
 *
 */
public class RealTimeClient {
  private static final Logger LOG = Logger.getLogger(RealTimeClient.class);

  protected final String host;
  protected final int port;
  protected final Client client;
  protected final String clientName;
  private ConcurrentMap<Context, AtomicLongMap<String>>  mondemandContent
        = new ConcurrentHashMap<Context, AtomicLongMap<String>>();
  private volatile boolean enabled = true;
  protected final int INTERVAL = 60000;
  protected boolean statsReset = true;

  /**
   * constructor for the singleton
   * @param clientName
   * @param host
   * @param port
   */
  public RealTimeClient(String clientName, String host, int port)
  {
    this.clientName = clientName;
    this.host = host;
    this.port = port;
    this.client = createClient();
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
   * @param context context
   * @param keyType key
   * @param value value
   */
  public void increment(Context context, String keyType, long value)
  {
    synchronized (keyType) {
      AtomicLongMap<String> stats = mondemandContent.get(context);
      if (stats == null)
      {
        stats = AtomicLongMap.create();
        mondemandContent.put(context, stats);
      }
      stats.addAndGet(keyType, value);
    }
  }

  /**
   * Increment the count for the map
   * @param context : context
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
          for (Map.Entry<Context, AtomicLongMap<String>> entry : mondemandContent.entrySet())
          {
            entry.getKey().addContext(client);
            AtomicLongMap<String> stats = entry.getValue();
            for (Map.Entry<String, Long> stat : stats.asMap().entrySet())
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

  public ConcurrentMap<Context, AtomicLongMap<String>> getMondemandContent() {
    return mondemandContent;
  }

  public Client getClient() {
    return client;
  }
}
