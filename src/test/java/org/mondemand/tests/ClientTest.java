/*======================================================================*
 * Copyright (c) 2008, Yahoo! Inc. All rights reserved.                 *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License.  Unless required    *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/
package org.mondemand.tests;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lwes.Event;
import org.lwes.EventFactory;
import org.lwes.MapEvent;
import org.lwes.emitter.BroadcastEmitterGroup;
import org.lwes.emitter.DatagramSocketEventEmitter;
import org.lwes.emitter.MulticastEventEmitter;
import org.lwes.emitter.UnicastEventEmitter;
import org.mondemand.Client;
import org.mondemand.Context;
import org.mondemand.ContextList;
import org.mondemand.ErrorHandler;
import org.mondemand.EventType;
import org.mondemand.Level;
import org.mondemand.LogMessage;
import org.mondemand.MondemandException;
import org.mondemand.SampleTrackType;
import org.mondemand.SamplesMessage;
import org.mondemand.StatType;
import org.mondemand.StatsMessage;
import org.mondemand.TraceId;
import org.mondemand.Transport;
import org.mondemand.TransportException;
import org.mondemand.log4j.MonDemandAppender;
import org.mondemand.transport.LWESTransport;
import org.mondemand.transport.StderrTransport;
import org.mondemand.util.ClassUtils;

public class ClientTest {
  // stub emitter group for LwesTransport
  class StubEmitterGroup extends BroadcastEmitterGroup {

    // since there may be multiple events sent for one set of metrics, collect
    // each one's data separately
    class EventData {
      public Map<String, String> eventTypes = new HashMap<String, String>();
      public Map<String, String> eventKeys = new HashMap<String, String>();
      public Map<String, Long> eventValues = new HashMap<String, Long>();
      public Map<String, Object> others = new HashMap<String, Object>();
      public EventData() { }
    }

    ArrayList<EventData> eventDataList = new ArrayList<EventData>();
    public int maxNumMetrics = 512;

    public StubEmitterGroup(DatagramSocketEventEmitter<?>[] emitters,
                            EventFactory eventFactory) {
      super(emitters, null, eventFactory);
    }

    /**
     * during emit, all we do is to capture the type/key/values we put in the
     * event to make sure the right key/values are put in the event, and nothing
     * extra is there. it populates 3 maps with the types/keys/values.
     *
     * @param event - the lwes event to be inspected
     */
    @Override
    public int emitToGroup(Event event) {
      // go through the event and populate the maps with the entries in the event
      // that start with "t", "k" or "v"
      MapEvent me = (MapEvent)event;
      EventData ed = new EventData();
      for(String key : me.getEventAttributes()) {
        if(key.startsWith("t")) {
          ed.eventTypes.put(key, me.getString(key));
        } else if (key.startsWith("k")) {
          ed.eventKeys.put(key, me.getString(key));
        } else if (key.startsWith("v")) {
          ed.eventValues.put(key, me.getInt64(key));
        } else {
          ed.others.put(key, me.get(key));
        }
      }

      eventDataList.add(ed);
      return 0;
    }

    public void clearMaps() {
      eventDataList.clear();
    }

    public String getEventTypes(int idx)
    {
      int fieldIdx = idx % maxNumMetrics;
      int mapIdx = idx / maxNumMetrics;

      EventData ed = eventDataList.get(mapIdx);
      return ed.eventTypes.get("t" + fieldIdx);
    }

    public String getEventKeys(int idx)
    {
      int fieldIdx = idx % maxNumMetrics;
      int mapIdx = idx / maxNumMetrics;

      EventData ed = eventDataList.get(mapIdx);
      return ed.eventKeys.get("k" + fieldIdx);
    }

    public Long getEventValues(int idx)
    {
      int fieldIdx = idx % maxNumMetrics;
      int mapIdx = idx / maxNumMetrics;

      EventData ed = eventDataList.get(mapIdx);
      return ed.eventValues.get("v" + fieldIdx);
    }

    public Object[] getOthers(String key)
    {
      Object o[] = new Object[eventDataList.size()];
      int i = 0;
      for (EventData ed : eventDataList)
      {
        Object v = ed.others.get(key);
        o[i++] = v;
      }
      return o;
    }

    public int eventTypesSize()
    {
      int sz = 0;
      for (EventData ed : eventDataList)
      {
        sz += ed.eventTypes.size();
      }
      return sz;
    }

    public int eventKeysSize()
    {
      int sz = 0;
      for (EventData ed : eventDataList)
      {
        sz += ed.eventKeys.size();
      }
      return sz;
    }

    public int eventValuesSize()
    {
      int sz = 0;
      for (EventData ed : eventDataList)
      {
        sz += ed.eventValues.size();
      }
      return sz;
    }

    public int getEventsNumber()
    {
      return eventDataList.size();
    }
  }

  private PrintStream oldError = null;

  @Before
  public void setUp() throws Exception {
    oldError = System.err;
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.reset();
    System.setErr(new PrintStream(stream));
  }

  @After
  public void tearDown() throws Exception {
    System.setErr(oldError);
  }

  private static Client createClientNoTransports(String name) {
    return new Client(name);
  }

  private static Client createClientNoTransports() {
    return createClientNoTransports("ClientTestSample");
  }

  private static Client createClient(String name) {
    Client client = createClientNoTransports(name);

    client.addTransport(new ClientTestTransport());

    return client;
  }

  private static Client createClient() {
    return createClient("ClientTestSample");
  }

  @Test
  public void testClientCreate() {
    Client c = createClientNoTransports(null);
    assertNotNull(c);
  }

  @Test
  public void testClientCreateWithHostname() throws UnknownHostException, MondemandException {
    String host = InetAddress.getLocalHost().getHostName();
    Client c = new Client("my-test", host);
    assertEquals(host, c.getContext("host"));
  }

  @Test
  public void testBasicStructures() throws MondemandException {
    // invalid contexts
    try {
      new Context(null, "value");
      // should throw exception, fail otherwise
      fail();
    } catch(MondemandException e) {}
    try {
      new Context("key", null);
      // should throw exception, fail otherwise
      fail();
    } catch(MondemandException e) {}

    Context ctxt = new Context("a", "b");
    assertEquals(ctxt.getKey(), "a");
    assertEquals(ctxt.getValue(), "b");

    Level level = new Level();
    assertNotNull(level);
    assertEquals(Level.STRINGS[0], "emerg");

    TraceId traceId = new TraceId();
    traceId.setId(12345);
    assertEquals(traceId.getId(), 12345L);
    TraceId traceId2 = new TraceId(23456);
    if(traceId.compareTo(traceId2) > 0) {}
    if(traceId2.compareTo(traceId) < 0) {}
    assertTrue(new TraceId(1).equals(new TraceId(1)));
    assertFalse(new TraceId(2).equals(new TraceId(4)));
    assertFalse(new TraceId(1).equals(new Long(1)));
    assertFalse(new TraceId(1).equals(null));

    TransportException e = new TransportException();
    e = new TransportException(new Exception());
    e = new TransportException("Hello", new Exception());
    e.toString();
  }

  @Test
  public void testIntrospection() {
    ClassUtils test = new ClassUtils();
    ClassUtils.getCallingClass(1000000);
    ClassUtils.getCallingLine(1000000);
    test.toString();
    ClassUtils.getCallingClass(-1000);
    ClassUtils.getCallingLine(-20000);
  }

  @Test
  public void testProgId() {
    Client client = createClient();
    client.setProgramId("test123");
    assertEquals(client.getProgramId(), "test123");
  }

  @Test
  public void testImmediateSendLevel() {
    Client client = createClient();
    client.setImmediateSendLevel(Level.EMERG);
    assertEquals(client.getImmediateSendLevel(), Level.EMERG);
  }

  @Test
  public void testNoSendLevel() {
    Client client = createClient();
    client.setNoSendLevel(Level.INFO);
    assertEquals(client.getNoSendLevel(), Level.INFO);
    assertTrue(client.levelIsEnabled(Level.WARNING, null));
    assertFalse(client.levelIsEnabled(Level.INFO, null));
    assertTrue(client.levelIsEnabled(Level.DEBUG, new TraceId(1)));
    assertFalse(client.levelIsEnabled(Level.DEBUG, TraceId.NULL_TRACE_ID));
  }

  @Test
  public void testSetContexts() throws MondemandException {
    Client client = createClient();
    for(int i=0; i<1000; ++i) {
      client.addContext("key" + i, "value" + i);
    }

    assertEquals(client.getContext("key999"), "value999");

    for(int i=0; i<1000; ++i) {
      client.removeContext("key" + i);
    }
    assertEquals(client.getContext("key1"), null);

    for(int i=0; i<1000; ++i) {
      client.addContext("key" + i, "value" + i);
    }

    int count = 0;
    Enumeration<String> keys = client.getContextKeys();
    while(keys.hasMoreElements()) {
      count++;
      keys.nextElement();
    }
    assertEquals(count, 1000);

    client.removeAllContexts();

    assertNull(client.getContext("key1"));
  }

  @Test
  public void testMaxNumMetrics() throws Exception {
    LWESTransport localLwesTransport = new LWESTransport(InetAddress.getLocalHost(), 9292, null);
    Client client = createLwesClient(localLwesTransport);
    StubEmitterGroup g = createStubEmitterGroup(localLwesTransport);

    int count = 100;
    client.addContext("test1", "test2");
    client.setMaxNumMetrics(count);

    for(int cnt=0; cnt<count; ++cnt) {
      client.increment("key_" + cnt, cnt);
    }

    client.flush(false); // don't clear data
    assertEquals(g.getEventsNumber(), 1);
    assertEquals(g.eventTypesSize(), count);
    g.clearMaps();

    // adding a new key makes the number of metrics 1 more than max
    client.increment("keyx", 1);

    client.flush(true);
    assertEquals(g.getEventsNumber(), 2);
    assertEquals(g.eventTypesSize(), count + 1);

    g.clearMaps();
    client.finalize();
  }

  @Test
  public void testPerformanceTrace() throws Exception {
    LWESTransport localLwesTransport = new LWESTransport(InetAddress.getLocalHost(), 9292, null);
    Client client = createLwesClient(localLwesTransport);
    StubEmitterGroup g = createStubEmitterGroup(localLwesTransport);

    long now = System.currentTimeMillis();
    long start0 = now;
    long start1 = now - 1000;
    long end0 = now + 5000;
    long end1 = now + 10000;
    Map<String, String> context = new HashMap<String, String>();

    context.put("context_test_key", "context_test_val");

    boolean ret = client.performanceTraceMessage("mondemand-java", "junit",
                                                 new String[]{ "a", "b" },
                                                 new long[]{ start0, start1 },
                                                 new long[]{ end0, end1 },
                                                 context);

    assertTrue(ret);
    assertEquals(g.getOthers("id")[0], "mondemand-java");
    assertEquals(g.getOthers("caller_label")[0], "junit");
    assertEquals(g.getOthers("num")[0], 2);
    assertEquals(g.getOthers("label0")[0], "a");
    assertEquals(g.getOthers("label1")[0], "b");
    assertEquals(g.getOthers("ctxt_num")[0], 1);
    assertEquals(g.getOthers("ctxt_k0")[0], "context_test_key");
    assertEquals(g.getOthers("ctxt_v0")[0], "context_test_val");
    assertEquals(g.getOthers("start0")[0], start0);
    assertEquals(g.getOthers("start1")[0], start1);
    assertEquals(g.getOthers("end0")[0], end0);
    assertEquals(g.getOthers("end1")[0], end1);
  }

  /**
   * create a client with a stub emitter, where we could inspect the entries
   * in the lwes that would be emitted (in real world). Then for a set of random
   * sample types (which covers all individual types and some combinations),
   * generate a set of random numbers to be added to some random keys, and finally
   * make sure that the desired stat types and their values are in the lwes
   * event, and there is no extras in the event.
   */
  @Test
  public void testSamplesMessages() throws Exception {
    LWESTransport localLwesTransport = new LWESTransport(InetAddress.getLocalHost(), 9292, null);
    Client client = createLwesClient(localLwesTransport);
    // add a second transport to test stats sample resets
    client.addTransport(localLwesTransport);
    StubEmitterGroup g = createStubEmitterGroup(localLwesTransport);

    Field sampleStats = client.getClass().getDeclaredField("samples");
    sampleStats.setAccessible(true);

    // sample types to check, contains all individual types and some combinations
    int[] sampleStatTypesToCheck = new int[]{
        SampleTrackType.MIN.value,
        SampleTrackType.MAX.value,
        SampleTrackType.AVG.value,
        SampleTrackType.MEDIAN.value,
        SampleTrackType.PCTL_75.value,
        SampleTrackType.PCTL_90.value,
        SampleTrackType.PCTL_95.value,
        SampleTrackType.PCTL_98.value,
        SampleTrackType.PCTL_99.value,
        SampleTrackType.SUM.value,
        SampleTrackType.COUNT.value,
        // min & max
        SampleTrackType.MIN.value | SampleTrackType.MAX.value,
        // average & 98 percentile
        SampleTrackType.AVG.value | SampleTrackType.PCTL_98.value,
        // 99 percentile & median
        SampleTrackType.PCTL_99.value | SampleTrackType.MEDIAN.value,
        // 95 percentile & 75 percentile
        SampleTrackType.PCTL_95.value | SampleTrackType.PCTL_75.value,
        // 98 percentile & sum and count
        SampleTrackType.PCTL_98.value | SampleTrackType.SUM.value |
        SampleTrackType.COUNT.value,
        // min & max & average & 90 percentile
        SampleTrackType.MIN.value | SampleTrackType.MAX.value |
        SampleTrackType.AVG.value | SampleTrackType.PCTL_90.value,
        // everything
        SampleTrackType.MIN.value | SampleTrackType.MAX.value |
        SampleTrackType.AVG.value | SampleTrackType.MEDIAN.value |
        SampleTrackType.PCTL_75.value | SampleTrackType.PCTL_90.value |
        SampleTrackType.PCTL_95.value | SampleTrackType.PCTL_98.value |
        SampleTrackType.PCTL_99.value | SampleTrackType.SUM.value |
        SampleTrackType.COUNT.value,
        // nothing
        0,
    };

    for(int typeIdx=0; typeIdx<sampleStatTypesToCheck.length; ++typeIdx) {
      String key = "SomeKey_" + typeIdx;

      // number of counter updates is random between MAX_SAMPLES_COUNT +/- 500 to make
      // sure we cover cases that samples size is > and < MAX_SAMPLES_COUNT
      int inputSize = (new Random()).nextInt(SamplesMessage.MAX_SAMPLES_COUNT) + 500;

      int total = 0;
      for(int val=1; val <= inputSize; ++val) {
        // value is random
        int rndValue = (new Random()).nextInt(10000);
        client.addSample(key, rndValue, sampleStatTypesToCheck[typeIdx]);
        total += rndValue;
      }

      // get the samples for the stats for the given key
      @SuppressWarnings("unchecked")
      ConcurrentHashMap<String, SamplesMessage> clientSamples = (ConcurrentHashMap<String,SamplesMessage>)sampleStats.get(client);
      ArrayList<Integer> samples = new ArrayList<Integer>(clientSamples.get(key).getSamples());
      Collections.copy(samples, clientSamples.get(key).getSamples());
      Collections.sort(samples);

      // trigger send()
      client.flush(true);

      // make sure the extra stat types specified are also set correctly
      int idx = 0;
      for(SampleTrackType trackType: SampleTrackType.values()) {
        // check if a specific trackType is set for the test case, if so
        // check the emitter's maps
        if( (sampleStatTypesToCheck[typeIdx] & trackType.value) ==
            trackType.value) {
          // we check something like "t1=gauge", "k1=SomeKey_0_min", "v1=10",
          // "t2=gauge", "k2=SomeKey_0_pctl_95", "v2=9500", all extra stats
          // are gauges
          assertEquals(g.getEventTypes(idx), "gauge");
          assertEquals(g.getEventKeys(idx), key + trackType.keySuffix);

          if(trackType.value == SampleTrackType.AVG.value) {
            assertEquals(g.getEventValues(idx).longValue(),
                total/inputSize);
          } else if(trackType.value == SampleTrackType.SUM.value) {
            assertEquals(g.getEventValues(idx).longValue(), total);
          } else if(trackType.value == SampleTrackType.COUNT.value) {
            assertEquals(g.getEventValues(idx).longValue(), inputSize);
          } else {
            assertEquals(g.getEventValues(idx).longValue(),
                samples.get( (int) ( (Math.min(inputSize, SamplesMessage.MAX_SAMPLES_COUNT)-1) *
                    trackType.indexInSamples)).intValue()  );
          }
          idx++;
        }
      }
      // finally make sure no other key/value/types are set.
      // * 2 is because we used two Transports above
      assertEquals(g.eventTypesSize(), idx * 2);
      assertEquals(g.eventKeysSize(), idx * 2);
      assertEquals(g.eventValuesSize(), idx * 2);

      // reset emitter
      g.clearMaps();
    }
    client.finalize();
  }

  /**
   * test invalid keys are not emitted
   * @throws Exception
   */
  @Test
  public void testStatsWithInvalidKeys() throws Exception {

    String[] invalidKeys = {"", "with space", "with_invalid_chars_%", "with_invalid_chars_$",
        "with_invalid_chars_+", "with_invalid_chars_:"};

    // stats
    for(String invalidKey : invalidKeys) {
      // create a client, with a transport's emitter that has the emit() stubbed out
      LWESTransport localLwesTransport = new LWESTransport(InetAddress.getLocalHost(), 9292, null);
      Client client = createLwesClient(localLwesTransport);
      StubEmitterGroup g = createStubEmitterGroup(localLwesTransport);

      // add stats
      try {
        client.increment(invalidKey, 100);
        // fail if we are not throwing exception
        fail();
      } catch(MondemandException e) {}
      client.flush(true);
      // nothing should have been emitted.
      assertEquals(g.eventTypesSize(), 0);
      assertEquals(g.eventKeysSize(), 0);
      assertEquals(g.eventValuesSize(), 0);

      // add samples
      try {
        client.addSample(invalidKey, 100, 1);
        // fail if we are not throwing exception
        fail();
      } catch(Exception e) {}
      client.flush(true);
      // nothing should have been emitted.
      assertEquals(g.eventTypesSize(), 0);
      assertEquals(g.eventKeysSize(), 0);
      assertEquals(g.eventValuesSize(), 0);
    }

  }

  /**
   * test invalid keys are not emitted
   * @throws Exception
   */
  @Test
  public void testStatsSamplesWithValidKeys() throws Exception {
    String[] validKeys = {"valid", "valid_with_underscore", "valid.with.dot", "valid-with-dash", "valid-with.numbers_090"};

    // stats
    for(String validKey : validKeys) {
      // create a client, with a transport's emitter that has the emit() stubbed out
      LWESTransport localLwesTransport = new LWESTransport(InetAddress.getLocalHost(), 9292, null);
      Client client = createLwesClient(localLwesTransport);
      StubEmitterGroup g = createStubEmitterGroup(localLwesTransport);

      // add stats
      client.finalize();
      client.increment(validKey, 100);
      client.flush();
      // one key/value should be emitted.
      assertEquals(g.eventTypesSize(), 1);
      assertEquals(g.eventKeysSize(), 1);
      assertEquals(g.eventValuesSize(), 1);
    }

    // samples
    for(String validKey : validKeys) {
      // create a client, with a transport's emitter that has the emit() stubbed out
      LWESTransport localLwesTransport = new LWESTransport(InetAddress.getLocalHost(), 9292, null);
      Client client = createLwesClient(localLwesTransport);
      StubEmitterGroup g = createStubEmitterGroup(localLwesTransport);

      // add samples
      client.addSample(validKey, 100, 1);
      client.flush();
      // one key/value should be emitted.
      assertEquals(g.eventTypesSize(), 1);
      assertEquals(g.eventKeysSize(), 1);
      assertEquals(g.eventValuesSize(), 1);
    }

  }

  /**
   * creates and returns a Client object
   * @return
   * @throws TransportException
   * @throws UnknownHostException
   */
  protected Client createLwesClient(LWESTransport LwesTransport) throws UnknownHostException, TransportException {
    Client client = createClientNoTransports("foofoo");
    client.addTransport(LwesTransport);
    return client;
  }

  /**
   * create a StubEmitterGroup
   * @param lwesTransport
   * @return
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  protected StubEmitterGroup createStubEmitterGroup(LWESTransport lwesTransport) throws NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    Field emitterGroup = lwesTransport.getClass().getDeclaredField("emitterGroup");
    emitterGroup.setAccessible(true);
    Field emitters = emitterGroup.get(lwesTransport).getClass().getDeclaredField("emitters");
    emitters.setAccessible(true);
    Field eventFactory = emitterGroup.get(lwesTransport).getClass().getSuperclass().getDeclaredField("factory");
    eventFactory.setAccessible(true);
    StubEmitterGroup group =
        new StubEmitterGroup(
            (DatagramSocketEventEmitter<?>[])emitters.get(emitterGroup.get(lwesTransport)),
            (EventFactory)eventFactory.get(emitterGroup.get(lwesTransport)));
     emitterGroup.set(lwesTransport, group);
     return group;
  }

  /**
   * test keyIsValid() method
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testIsValid() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    Client client = createClientNoTransports();
    Field examinedKeys = client.getClass().getDeclaredField("examinedKeys");
    examinedKeys.setAccessible(true);

    String[] validKeys = {"valid", "valid_with_underscore", "valid.with.dot", "valid-with-dash",
        "valid_with_numbers_090", "valid_with-all.chars090"};
    for(String validKey : validKeys) {
      assertTrue(Client.isKeyValid(validKey));
    }
    String[] invalidKeys = {null, "", "with space", "with_invalid_chars_%", "with_invalid_chars_$",
        "with_invalid_chars_+", "with_invalid_chars_:", "with_invalid_chars_="};
    for(String invalidKey : invalidKeys) {
      assertFalse(Client.isKeyValid(invalidKey));
    }

    // running the same for loops should use the map
    for(String validKey : validKeys) {
      assertTrue(Client.isKeyValid(validKey));
      @SuppressWarnings("unchecked")
      ConcurrentHashMap<String, Boolean> b = (ConcurrentHashMap<String, Boolean>)examinedKeys.get(validKey);
      assertTrue( b.get(validKey) );
    }
    for(String invalidKey : invalidKeys) {
      assertFalse(Client.isKeyValid(invalidKey));
      // null and empty string are not put in to the map
      if(invalidKey != null && !invalidKey.equals("")) {
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, Boolean> b = (ConcurrentHashMap<String, Boolean>)examinedKeys.get(invalidKey);
        assertFalse(b.get(invalidKey));
      }
    }

  }

  /**
   * this will test the auto amit feature in the client, one time
   * with reseting the stats after each emit and one time with keeping the stats
   */
  @Test
  public void testAutoEmitter() throws Exception  {
    // auto emit once every second
    boolean[] keepOrDropStats = new boolean[]{true, false};
    for(int kods = 0; kods < keepOrDropStats.length; kods++) {
      Client client = new Client("ClientTestSample", true, keepOrDropStats[kods], 1);
      LWESTransport localLwesTransport = new LWESTransport(InetAddress.getLocalHost(), 9292, null);
      client.addTransport(localLwesTransport);
      StubEmitterGroup g = createStubEmitterGroup(localLwesTransport);

      // create a bunch of regular counter stats
      int Count = 500;
      for(int cnt=0; cnt<Count; ++cnt) {
        client.increment("key_" + cnt, cnt);
      }
      // create a bunch of samples
     for(int cnt=0; cnt<Count; ++cnt) {
        client.addSample("samplekey_" + cnt, cnt, SampleTrackType.AVG.value);
      }
      // now sleep for 1.2 sec to make sure auto emit kicks in
      Thread.sleep(1500);

      // make sure Count*2 number of type/key/values are emitted, one for regular
      // counter,  and one for avg value of the samples.
      assertEquals(g.eventTypesSize(), Count*2);
      assertEquals(g.eventKeysSize(), Count*2);
      assertEquals(g.eventValuesSize(), Count*2);

      // now check the type/key/values in the emitter object
      for(int idx=0; idx<Count*2; ++idx) {
        assertNotNull(g.getEventKeys(idx));
        // extract the key
        String key = g.getEventKeys(idx);
        int val = 0;
        if(key.startsWith("key_")) {
          // regular key
          assertEquals(g.getEventTypes(idx), "counter");
          val = Integer.parseInt(key.substring( "key_".length() ));
        } else if(key.startsWith("samplekey_")) {
          // average for sample key
          assertEquals(g.getEventTypes(idx), "gauge");
          String s = key.substring( "samplekey_".length() );
          val = Integer.parseInt(s.substring(0, s.indexOf("_avg")));
        } else {
          // should never happen.
          assertNotNull(null);
        }
        assertEquals(g.getEventValues(idx).longValue(), val);
      }

      assertEquals(g.getEventsNumber(), 2);
      assertEquals(g.getOthers("prog_id")[1], g.getOthers("prog_id")[0]);
      assertThat(g.getOthers("num")[1], not(equalTo(g.getOthers("num")[0])));
      g.clearMaps();

      // now sleep for 1.2 sec without emiting anything
      Thread.sleep(1200);
      if(keepOrDropStats[kods]) {
        // we are reseting all stats, there should not be anything emitted
        assertEquals(g.eventTypesSize(), 0);
        assertEquals(g.eventKeysSize(), 0);
        assertEquals(g.eventValuesSize(), 0);
      } else {
        // we are not reseting the stats, the same keys with the same values should be emitted
        // the value for averages for sample counter should be 0 since average is a gauge
        // sample is cleared therefore we any got to Count not Count*2
        for(int idx=0; idx<Count; ++idx) {
          assertNotNull(g.getEventKeys(idx));
          // extract the key
          String key = g.getEventKeys(idx);
          int val = 0;
          if(key.startsWith("key_")) {
            // regular key
            assertEquals(g.getEventTypes(idx), "counter");
            val = Integer.parseInt(key.substring( "key_".length() ));
          } else if(key.startsWith("samplekey_")) {
            // average for sample key, should be reset to 0
            assertEquals(g.getEventTypes(idx), "gauge");
            val = 0;
          } else {
            // should never happen.
            assertNotNull(null);
          }
          assertEquals(g.getEventValues(idx).longValue(), val);
        }
      }

      client.finalize();
    }
  }

  /**
   * this will test the addTransportsFromConfigFile method where we get the
   * mondemand address from a file.
   */
  @Test
  public void testClientAddTransportFromFile() {
    Client client = createClientNoTransports();
    File mondemandConfigFile = null;
    FileOutputStream output = null;
    try {
      // non-exisitng config file
      String nonExistingFileName = "non_existing_file" + (new Random()).nextInt();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(nonExistingFileName);
        assertTrue(false);
      } catch(Exception e) {}

      // missing host
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      output.write("MONDEMAND_PORT=\"3344\"".getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        assertTrue(false);
      } catch(Exception e) {}
      mondemandConfigFile.deleteOnExit();

      // missing port
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      output.write("MONDEMAND_ADDR=\"127.0.0.1\"".getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        assertTrue(false);
      } catch(Exception e) {}
      mondemandConfigFile.deleteOnExit();

      // bad port
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      output.write("MONDEMAND_ADDR=\"127.0.0.1\"\nMONDEMAND_PORT=\"dds\"".getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        assertTrue(false);
      } catch(Exception e) {}
      mondemandConfigFile.deleteOnExit();

      // invalid host
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      output.write("MONDEMAND_ADDR=\"450.1.2.3\"\nMONDEMAND_PORT=\"1234\"".getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        assertTrue(false);
      } catch(Exception e) {}
      mondemandConfigFile.deleteOnExit();

      // invalid ttl, not numeric
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      output.write("MONDEMAND_ADDR=\"127.0.0.1\"\nMONDEMAND_PORT=\"1234\"\nMONDEMAND_TTL=\"abc\"".getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        assertTrue(false);
      } catch(Exception e) {}
      mondemandConfigFile.deleteOnExit();

      // invalid ttl, less than min
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      output.write("MONDEMAND_ADDR=\"127.0.0.1\"\nMONDEMAND_PORT=\"1234\"\nMONDEMAND_TTL=\"-1\"".getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        assertTrue(false);
      } catch(Exception e) {}
      mondemandConfigFile.deleteOnExit();

      // invalid ttl, more than max
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      output.write("MONDEMAND_ADDR=\"127.0.0.1\"\nMONDEMAND_PORT=\"1234\"\nMONDEMAND_TTL=\"33\"".getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        assertTrue(false);
      } catch(Exception e) {}
      mondemandConfigFile.deleteOnExit();

      // everything valid
      mondemandConfigFile = File.createTempFile("mondemand_config_", ".tmp");
      output = new FileOutputStream(mondemandConfigFile);
      List<String> addresses = new ArrayList<String>();
      addresses.add("127.0.0.1");
      addresses.add("127.0.0.2");
      addresses.add("224.1.2.200");
      StringBuilder allAddresses = new StringBuilder();
      for(String addr: addresses) {
        allAddresses.append(", ").append(addr);
      }
      output.write( ("MONDEMAND_ADDR=\"" + allAddresses.toString().substring(1)
          + "\"\nMONDEMAND_PORT=\"1234\"\nMONDEMAND_TTL=\"10\"") .getBytes());
      output.close();
      try {
        // should throw exception
        client.addTransportsFromConfigFile(mondemandConfigFile.getAbsolutePath());
        // verify transports
        Field transports = client.getClass().getDeclaredField("transports");
        transports.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<EventType, List<Transport>> clientTransports =
          (ConcurrentHashMap<EventType, List<Transport>>)transports.get(client);
        assertEquals(clientTransports.size(), EventType.values().length);
        for (List<Transport> transportList : clientTransports.values()) {
          for(Transport t: transportList) {
            int cnt = 0;
            Field emitterGroup = t.getClass().getDeclaredField("emitterGroup");
            emitterGroup.setAccessible(true);
            Field emitters = emitterGroup.get(t).getClass().getDeclaredField("emitters");
            emitters.setAccessible(true);
            String address;
            int port;
            DatagramSocketEventEmitter<?>[] dsee_arr =
              (DatagramSocketEventEmitter<?>[])emitters.get(emitterGroup.get(t));
            for (DatagramSocketEventEmitter<?> dsee : dsee_arr) {
              if (dsee instanceof UnicastEventEmitter) {
                address = ((UnicastEventEmitter)dsee).getAddress().getHostAddress();
                port = ((UnicastEventEmitter)dsee).getPort();
              } else {
                address = ((MulticastEventEmitter)dsee).getMulticastAddress().getHostAddress();
                port = ((MulticastEventEmitter)dsee).getMulticastPort();
                int ttl = ((MulticastEventEmitter)dsee).getTimeToLive();
                assertEquals(ttl, 10);
              }
              assertEquals(address, addresses.get(cnt++));
              assertEquals(port, 1234);
            }
          }
        }
      } catch(Exception e) {
        // should not throw exception
        assertTrue(false);
      }
      mondemandConfigFile.deleteOnExit();

    } catch(Exception e) {
      // should no see any exceptions
      fail("this should never happen!");
    }
  }

  @Test
  public void testEmptyFlush() {
    Client client = createClient();
    client.flush();
    client.flushLogs();
    client.flush(true);
  }

  @Test
  public void testIncrement() throws MondemandException {
    Client c = createClientNoTransports("testIncrement");
    ClientTestTransport t = new ClientTestTransport();
    c.addTransport(t);
    c.increment();
    c.increment(5);
    c.increment("testIncrement");
    c.increment("testIncrement2", 10);
    c.flush(true);
    assertEquals(t.stats.length, 3);
  }

  @Test
  public void testDecrement() throws MondemandException {
    Client client = createClientNoTransports();
    ClientTestTransport transport = new ClientTestTransport();
    client.addTransport(transport);
    client.decrement();
    client.decrement(5);
    client.decrement("testDecrement");
    client.decrement("testDecrement2", 10);
    client.flush(true);
    assertEquals(transport.stats.length, 3);
  }

  @Test
  public void testSetKey() throws MondemandException {
    Client client = createClientNoTransports();
    ClientTestTransport transport = new ClientTestTransport();
    client.addTransport(transport);
    client.setKey("testSetKey", 123);
    client.setKey("testSetKeyLong", 123L);
    // setting invalid keys should be ignored
    try {
      client.setKey("invalid key", 234);
      // fail if we are not throwing exception
      fail();
    } catch (MondemandException e) {}
    client.flush();
    assertEquals(transport.stats.length, 2);
  }

  @Test
  public void testLogMessages() {
    Client client = createClientNoTransports();
    ClientTestTransport transport = new ClientTestTransport();
    client.addTransport(transport);
    client.setNoSendLevel(Level.DEBUG);
    client.setImmediateSendLevel(Level.OFF);
    client.emerg("emerg");
    client.alert("alert");
    client.crit("crit");
    client.error("error");
    client.warning("warning");
    client.notice("notice");
    client.info("info");
    client.debug("debug");
    client.flushLogs();
    assertEquals(transport.logs.length, 7);
  }

  @Test
  public void testLogGeneric() {
    Client client = createClientNoTransports();
    ClientTestTransport transport = new ClientTestTransport();
    client.addTransport(transport);
    client.setNoSendLevel(Level.DEBUG);
    client.setImmediateSendLevel(Level.OFF);
    client.log("logGeneric", 123, Level.ALERT, null, "Test Message", null);
    client.flushLogs();
    assertEquals(transport.logs[0].getFilename(), "logGeneric");
    assertEquals(transport.logs[0].getLine(), 123);
    assertEquals(transport.logs[0].getLevel(), Level.ALERT);
    assertEquals(transport.logs[0].getMessage(), "Test Message");
    client.log(Level.ERROR, null, "test 2", null);
    client.log(Level.DEBUG, null, null, null);
    client.log("bad", 1, 100, null, "Bogus Message", null);
    client.log(null, 5, Level.ALERT, new TraceId(), "Missing filename", null);
    client.log("generic", 123, Level.DEBUG, new TraceId(123), "Trace Message", null);
    client.setImmediateSendLevel(Level.ALERT);
    client.log("send Now", 123, Level.EMERG, null, "Immediate Send", null);

    client.setNoSendLevel(Level.INFO);
    for(int i=0; i<100; ++i) {
      client.log("msg" + i, 1, Level.ERROR, null, "Bundle", null);
    }
  }

  @Test
  public void testStderrTransport() throws Exception {
    Client client = createClientNoTransports();
    Transport t = new StderrTransport();
    client.addTransport(t);
    for(int i=0; i<1000; ++i) {
      client.log("testStderrTransport", 123, Level.CRIT, null, "Test Message", null);
    }
    client.log(Level.CRIT, new TraceId(1), "Test", null);
    client.flushLogs();

    client.setKey("a", 123);
    client.flush();

    PrintStream currentError = System.err;
    System.setErr(null);
    client.log(Level.CRIT, null, "test", null);
    client.setKey("b", 2);
    client.flushLogs();
    client.flush();
    System.setErr(currentError);

    client.finalize();
    client = null;

    t.sendLogs(null, null, null);
    t.send(null, null, null, null, null);

    client = createClientNoTransports("ClientTest");
    client.flushLogs();

  }

  @Test
  public void testLwesTransport() throws Exception {
    Client client = createClientNoTransports();
    client.addContext("test1", "test2");

    InetAddress address = InetAddress.getLocalHost();
    Transport t = new LWESTransport(address, 9191, null);
    client.addTransport(t);

    new LWESTransport(InetAddress.getByName("224.1.1.111"), 80, null, 100);

    for(int i=0; i<1000; ++i) {
      client.log("testLwesTransport", 123, Level.CRIT, null, "Test Message", null);
    }
    client.flushLogs();

    client.setNoSendLevel(Level.DEBUG);
    client.setImmediateSendLevel(Level.ALERT);
    for(int i=0; i<5000; ++i) {
      client.warning("repeat in an endless loop");
    }

    // test multicast
    InetAddress maddr = InetAddress.getByName("224.0.0.1");
    Transport mt = new LWESTransport(maddr, 9191, null);
    client.addTransport(mt);
    for(int i=0; i<1000; ++i) {
      client.log("testLwesTransport", 345, Level.ALERT, null, "Test Message", null);
    }
    for (int i=0; i < 5; ++i) {
      client.increment("aCounter");
    }
    client.setKey("aGauge", 123);

    client.setKey(StatType.Gauge, "anotherGauge", 566);
    client.setKey(StatType.Counter, "anotherCounter", 555);
    client.increment("yac",25);
    client.flush();

    client.setNoSendLevel(Level.INFO);
    client.log("testTraceId", 555, Level.DEBUG, new TraceId(3117), "did it trace?",null);

    t.sendLogs(null, null, null);
    t.send(null, null, null, null, null);
    t.shutdown();
  }

  @Test
  public void testTraceMessages () throws Exception {
    Client c = createClientNoTransports("tracer");
    InetAddress address = InetAddress.getLocalHost();
    Transport t = new LWESTransport(address, 9292, null);
    c.addTransport(t);
    Map<String,String> myContext = new HashMap<String,String>();
    myContext.put("apple","cat");
    myContext.put("dog","5");
    assertEquals (c.traceMessage ("owner_1", "trace_id_1",
                                  "message 1", myContext), true);

    /* context only has trace, so traceMessage will fail */
    myContext.put("mondemand.trace_id", "trace_id_2");
    assertEquals (c.traceMessage ("message 2", myContext), false);

    /* now it has an owner and a trace so will work */
    myContext.put("mondemand.owner", "owner_2");
    assertEquals (c.traceMessage ("message 3", myContext), true);
    t.shutdown();
  }

  @Test
  public void testErrorHandler() {
    Client client = createClient();
    ErrorHandler current = client.getErrorHandler();
    ErrorHandler h = new TestErrorHandler();
    client.setErrorHandler(h);
    client.log("bad", 1, 100, null, "Bogus Message", null);
    client.setErrorHandler(current);
  }

  @Test
  public void testInternalProblems() throws Exception {
    Client client = createClientNoTransports();
    ClientTestTransport transport = new ClientTestTransport();
    client.addTransport(transport);

    Field contexts = client.getClass().getDeclaredField("contexts");
    contexts.setAccessible(true);
    Field transports = client.getClass().getDeclaredField("transports");
    transports.setAccessible(true);
    Field stats = client.getClass().getDeclaredField("stats");
    stats.setAccessible(true);
    Field messages = client.getClass().getDeclaredField("messages");
    messages.setAccessible(true);

    contexts.set(client, null);
    client.addContext("test1", "test2");

    contexts.set(client, null);
    client.getContextKeys();

    client.addTransport(null);

    stats.set(client, null);
    client.increment();

    stats.set(client, null);
    client.setKey(null, 123);

    contexts.set(client, null);
    client.flushLogs();
    client.flush();
    client.getContextKeys();

    stats.set(client,null);
    client.flushLogs();
    client.flush();
    client.setKey(null, 1);

    messages.set(client, null);
    client.log(Level.DEBUG, null, "abc123", new String[] { "test" });
  }

  @Test
  public void testFinalize() {
    Client client = createClientNoTransports();
    client.setErrorHandler(new TestErrorHandler());
    Transport t = new BogusTransport();
    client.addTransport(t);
    Transport t2 = new BogusTransport();
    client.addTransport(t2);
    client.finalize();
    client = null;
    client = createClientNoTransports("ClientTest");
  }

  @Test
  public void testAppender() {
    MonDemandAppender appender = new MonDemandAppender();
    appender.setProgId("testAppender");
    assertEquals(appender.getProgId(), "testAppender");
    appender.setAddress("127.0.0.1");
    assertEquals(appender.getAddress(), "127.0.0.1");
    appender.setPort(9191);
    assertEquals(appender.getPort(), 9191);
    appender.setInterface(null);
    assertEquals(appender.getInterface(), null);
    appender.setTtl(1);
    assertEquals(appender.getTtl(), 1);
    appender.setImmediateSendLevel(Level.CRIT);
    assertEquals(appender.getImmediateSendLevel(), Level.CRIT);
    assertFalse(appender.requiresLayout());

    appender.setImmediateSendLevel(Level.DEBUG);

    appender.setAddress("299293939323213231321321");
    appender.activateOptions();
    appender.setAddress("127.0.0.1");
    appender.activateOptions();

    HashMap<String,Object> properties = new HashMap<String,Object>();
    properties.put("key1", "value1");

    LoggingEvent event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, org.apache.log4j.Level.ERROR,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, org.apache.log4j.Level.WARN,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, org.apache.log4j.Level.FATAL,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, org.apache.log4j.Level.INFO,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, org.apache.log4j.Level.DEBUG,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, org.apache.log4j.Level.TRACE,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, null,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    // this breaks the NDC properties
    properties.put("key1", new java.util.BitSet());
    event = new LoggingEvent("ClientTest", Logger.getRootLogger(), 0L, org.apache.log4j.Level.INFO,
        "test", "test", null,
        "abc", new LocationInfo("ClientTest", "ClientTest", "testAppender", "398"), properties);
    appender.doAppend(event);

    appender.close();
  }

  /**
   * This is a test transport that simply exposes the contents of the
   * messages to the test program.
   * @author mlum
   *
   */
  public static class ClientTestTransport implements Transport
  {
    public LogMessage[] logs = new LogMessage[0];
    public StatsMessage[] stats = new StatsMessage[0];
    public SamplesMessage[] samples = new SamplesMessage[0];
    public TraceId traceId = null;

    @Override
    public void sendLogs (String programId,
                          LogMessage[] messages,
                          Context[] contexts)
    {
      logs = new LogMessage[messages.length];
      for(int i=0; i<logs.length; ++i) {
        logs[i] = messages[i];
        if(logs[i].getTraceId() != null) {
          traceId = logs[i].getTraceId();
        }
      }
    }

    @Override
    public void send (String programId,
        StatsMessage[] messages,
        SamplesMessage[] samples,
        Context[] contexts,
        Integer maxNumMetrics) {
      stats = new StatsMessage[messages.length];
      for(int i=0; i<stats.length; ++i) {
        stats[i] = messages[i];
      }
      if (samples != null) {
        this.samples = new SamplesMessage[samples.length];
        for(int i=0; i<this.samples.length; ++i) {
          this.samples[i] = samples[i];
        }
      }
    }

    @Override
    public void sendTrace (String programId,
                           Context[] contexts)
      throws TransportException
    {
      /* FIXME: do something here? */
    }

    @Override
    public void sendPerformanceTrace(String id, String callerLabel,
                                     String[] label, long[] start,
                                     long[] end, Context[] contexts)
      throws TransportException
    {
      /* FIXME: do something here? */
    }

    @Override
    public void shutdown() {

    }
  }

  public static class BogusTransport implements Transport
  {
    @Override
    public void sendLogs (String programId,
                          LogMessage[] messages,
                          Context[] contexts)
      throws TransportException
    {
      throw new TransportException("BogusTransport");
    }

    @Override
    public void send (String programId,
        StatsMessage[] messages,
        SamplesMessage[] samples,
        Context[] contexts,
        Integer maxNumMetrics) throws TransportException
    {
      throw new TransportException("BogusTransport");
    }

    @Override
    public void sendTrace (String programId,
                           Context[] contexts)
      throws TransportException
    {
      throw new TransportException("BogusTransport");
    }

    @Override
    public void sendPerformanceTrace(String id, String callerLabel,
                                     String[] label, long[] start,
                                     long[] end, Context[] contexts)
      throws TransportException
    {
      throw new TransportException("BogusTransport");
    }

    @Override
    public void shutdown() throws TransportException
    {
      throw new TransportException("BogusTransport");
    }
  }

  @Test
  public void testIncrementMap() throws MondemandException
  {
    Client client = createClient();
    Context context = new Context("k1", "v1");
    ContextList contexts = new ContextList();
    contexts.addContext(context);
    for (int i=0; i<1000; i++)
    {
      client.increment(contexts, "key1");
      client.increment(contexts, "key2", 100l);
      // bad keys should be ignored
      try {
        client.increment(contexts, "invalid_key_=", 200L);
        // fail if we are not throwing exception
        fail();
      } catch(MondemandException e) {}
    }
    ContextList contextsCopy = new ContextList();
    contextsCopy.addContext(new Context("k1", "v1"));
    Context context2 = new Context("k1", "v2");
    ContextList contexts2 = new ContextList();
    contexts2.addContext(context2);
    for (int i=0; i<1000; i++)
    {
      client.increment(contexts2, "key1", 10l);
    }
    assertTrue(client.getContextStats().containsKey(contexts));
    assertTrue(client.getContextStats().containsKey(contextsCopy));
    assertTrue(client.getContextStats().get(contextsCopy).containsKey("key1"));
    assertEquals(1000l, client.getContextStats().get(contextsCopy).get("key1"));
    assertTrue(client.getContextStats().containsKey(contextsCopy));
    assertTrue(client.getContextStats().get(contextsCopy).containsKey("key2"));
    assertEquals(100000l, client.getContextStats().get(contextsCopy).get("key2"));

    assertTrue(client.getContextStats().containsKey(contexts2));
    assertTrue(client.getContextStats().get(contexts2).containsKey("key1"));
    assertEquals(10000l, client.getContextStats().get(contexts2).get("key1"));

  }

  @Test
  public void testMultiThreadIncrement() throws InterruptedException, MondemandException
  {
    final Client client = createClient();
    Context context1 = new Context("k1", "v1");
    Context context2 = new Context("k2", "v2");
    final ContextList contexts = new ContextList();
    contexts.addContext(context1);
    contexts.addContext(context2);
    for (int j=0; j<10; j++)
    {
      class IncrementThread implements Runnable {
        @Override
        public void run()
        {
          for (int i=0; i< 1000; i++)
          {
            try {
              client.increment(contexts, "key1");
            } catch (MondemandException e) {
              e.printStackTrace();
            }
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

      assertEquals(3000 * (j+1), client.getContextStats().get(contexts).get("key1"));
      client.flush();
    }
  }

  @Test
  public void testFlush() throws MondemandException
  {
    Client client = createClient();
    Context context = new Context("k1", "v1");
    ContextList contexts = new ContextList();
    contexts.addContext(context);
    for (int i=0; i<1000; i++)
    {
      client.increment(contexts, "key1");
      client.increment(contexts, "key2", 100l);
    }
    client.flush();
    try
    {
      Thread.sleep(60);
    }
    catch (InterruptedException ie)
    {
    }
    assertEquals(1000l, client.getContextStats().get(contexts).get("key1"));
  }

  @Test
  public void testMultiThreadAddSample() throws InterruptedException
  {
    final Client client = createClient();
    for (int j=0; j<10; j++)
    {

      class IncrementThread implements Runnable {
        @Override
        public void run()
        {
          for (int i=0; i< 100; i++)
          {
            try {
              client.addSample("k1", 1, 328);
            } catch (MondemandException e) {
              e.printStackTrace();
            }
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

      assertEquals(300l, client.getSamples().get("k1").getCounter());
      client.flush();
    }
  }

  public static class TestErrorHandler implements ErrorHandler
  {
    @Override
    public void handleError (String error)
    {

    }

    /**
     * Writes MonDemand errors that have an associated exception
     * to standard error.
     */
    @Override
    public void handleError (String error, Exception e)
    {
    }

    @Override
    public void handleError(String error, Exception e, Context[] messageContext) {

    }
  }
}
