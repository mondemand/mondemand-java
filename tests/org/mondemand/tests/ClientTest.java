package org.mondemand.tests;

import org.mondemand.Client;
import org.mondemand.Context;
import org.mondemand.ErrorHandler;
import org.mondemand.Level;
import org.mondemand.LogMessage;
import org.mondemand.StatsMessage;
import org.mondemand.TraceId;
import org.mondemand.Transport;
import org.mondemand.TransportException;
import org.mondemand.transport.StderrTransport;
import org.mondemand.transport.LWESTransport;
import org.mondemand.util.ClassUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.net.InetAddress;
import java.util.Enumeration;
import java.lang.reflect.Field;

public class ClientTest {
	private static Client client = new Client("ClientTest");
	private static boolean transportIsSet = false;
	private static ClientTestTransport transport = null;
	
	// internal structures to play with
	private Field contexts = null;
	private Field transports = null;
	private Field stats = null;
	private Field messages = null;
	
	@Before
	public void setUp() throws Exception {
		if(!transportIsSet) {
			transport = new ClientTestTransport();
			client.addTransport(transport);
			transportIsSet = true;
		}

		// allow ourselves to mess with the internal structures
		contexts = client.getClass().getDeclaredField("contexts");
		contexts.setAccessible(true);
		transports = client.getClass().getDeclaredField("transports");
		transports.setAccessible(true);
		stats = client.getClass().getDeclaredField("stats");
		stats.setAccessible(true);
		messages = client.getClass().getDeclaredField("messages");
		messages.setAccessible(true);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testClientCreate() {
		Client c = new Client(null);
		assertNotNull(c);
	}

	@Test
	public void testBasicStructures() {
		Context ctxt = new Context();
		ctxt.setKey("a");
		ctxt.setValue("b");
		assertEquals(ctxt.getKey(), "a");
		assertEquals(ctxt.getValue(), "b");
		
		Level level = new Level();
		assertNotNull(level);
		assertEquals(Level.STRINGS[0], "emerg");
		
		TraceId traceId = new TraceId();
		traceId.setId(12345);
		assertEquals(traceId.getId(), 12345);
		TraceId traceId2 = new TraceId(23456);
		if(traceId.compareTo(traceId2) > 0) {}
		if(traceId2.compareTo(traceId) < 0) {}
		
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
		client.setProgramId("test123");
		assertEquals(client.getProgramId(), "test123");
	}
	
	@Test
	public void testImmediateSendLevel() {
		client.setImmediateSendLevel(Level.EMERG);
		assertEquals(client.getImmediateSendLevel(), Level.EMERG);
	}
	
	@Test
	public void testNoSendLevel() {
		client.setNoSendLevel(Level.INFO);
		assertEquals(client.getNoSendLevel(), Level.INFO);
		assertTrue(client.levelIsEnabled(Level.WARNING, null));
		assertFalse(client.levelIsEnabled(Level.INFO, null));
		assertTrue(client.levelIsEnabled(Level.DEBUG, new TraceId(1)));
		assertFalse(client.levelIsEnabled(Level.DEBUG, TraceId.NULL_TRACE_ID));
	}
	
	@Test
	public void testSetContexts() {
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
	public void testEmptyFlush() {
		client.flush();
		client.flushStats();
		client.flushLogs();
		client.flushStats(false);
	}
	
	@Test
	public void testIncrement() {
		client.increment();
		client.increment(5);
		client.increment("testIncrement");
		client.increment("testIncrement2", 10);
		client.flushStats();
		assertEquals(transport.stats.length, 3);
	}
	
	@Test
	public void testDecrement() {
		client.decrement();
		client.decrement(5);
		client.decrement("testDecrement");
		client.decrement("testDecrement2", 10);
		client.flushStats();
		assertEquals(transport.stats.length, 3);
	}
	
	@Test
	public void testSetKey() {
		client.setKey("testSetKey", 123);
		client.setKey("testSetKeyLong", 123L);
		client.flushStats();
		assertEquals(transport.stats.length, 2);
	}
	
	@Test
	public void testLogMessages() {
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
	public void testStderrTransport() {
		Transport t = new StderrTransport();
		client.addTransport(t);
		for(int i=0; i<1000; ++i) {
			client.log("testStderrTransport", 123, Level.CRIT, null, "Test Message", null);
		}
		client.log(Level.CRIT, new TraceId(1), "Test", null);
		client.flushLogs();
		
		client.setKey("a", 123);
		client.flushStats();
		
		client.finalize();
		client = null;
		
		client = new Client("ClientTest");
	}
	
	@Test
	public void testLwesTransport() throws Exception {
		client.addContext("test1", "test2");
		
		new LWESTransport(null, 0, null);
		
		InetAddress address = InetAddress.getLocalHost();
		Transport t = new LWESTransport(address, 9191, null);
		client.addTransport(t);
		for(int i=0; i<1000; ++i) {
			client.log("testLwesTransport", 123, Level.CRIT, null, "Test Message", null);
		}
		client.flushLogs();
			
		// test multicast
		InetAddress maddr = InetAddress.getByName("224.0.0.1");
		Transport mt = new LWESTransport(maddr, 9191, null);
		client.addTransport(mt);
		for(int i=0; i<1000; ++i) {
			client.log("testLwesTransport", 345, Level.ALERT, null, "Test Message", null);
		}
		
		client.setKey("a", 123);
		client.flushStats();
		
		t.sendLogs(null, null, null);
		t.sendStats(null,null, null);
	}
	
	@Test
	public void testTransportShutdown() {
		transport.shutdown();
	}
	
	@Test
	public void testErrorHandler() {
		ErrorHandler current = client.getErrorHandler();
		ErrorHandler h = new TestErrorHandler();
		client.setErrorHandler(h);
		client.log("bad", 1, 100, null, "Bogus Message", null);
		client.setErrorHandler(current);
	}
	
	@Test
	public void testInternalProblems() throws Exception {
		contexts.set(client, null);
		client.addContext("test1", "test2");
		
		contexts.set(client, null);
		client.getContextKeys();
		
		transports.set(client, null);
		client.addTransport(null);
		
		stats.set(client, null);
		client.increment();
		
		stats.set(client, null);
		client.setKey(null, 123);
		
		contexts.set(client, null);
		client.flushLogs();
		client.flushStats();
		client.getContextKeys();
		
		stats.set(client,null);
		client.flushLogs();
		client.flushStats();
		client.setKey(null, 1);
		
		transports.set(client, null);
		client.flushLogs();
		client.flushStats();
		client.addTransport(null);
		
		messages.set(client, null);
		client.log(Level.DEBUG, null, "abc123", new String[] { "test" });
		
	}
	
	@Test
	public void testFinalize() {
		client.setErrorHandler(new TestErrorHandler());
		Transport t = new BogusTransport();
		client.addTransport(t);
		Transport t2 = new BogusTransport();
		client.addTransport(t2);
		client.finalize();
		client = null;
		client = new Client("ClientTest");
	}
	
	/**
	 * This is a test transport that simply exposes the contents of the
	 * messages to the test program.
	 * @author mlum
	 *
	 */
	public class ClientTestTransport implements Transport {
		public LogMessage[] logs = new LogMessage[0];
		public StatsMessage[] stats = new StatsMessage[0];
		public TraceId traceId = null;
		
		public void sendLogs(String programId, LogMessage[] messages, Context[] contexts) {
			logs = new LogMessage[messages.length];
			for(int i=0; i<logs.length; ++i) {
				messages[i].toString();
				logs[i] = messages[i];
				if(logs[i].getTraceId() != null) {
					traceId = logs[i].getTraceId();
				}
			}
		}
		
		public void sendStats(String programId, StatsMessage[] messages, Context[] contexts) {
			stats = new StatsMessage[messages.length];
			for(int i=0; i<stats.length; ++i) {
				messages[i].toString();
				stats[i] = messages[i];
				stats[i].getKey();
			}
		}
		
		public void shutdown() {
			
		}
	}
	
	public class BogusTransport implements Transport {
		public void sendLogs(String programId, LogMessage[] messages, Context[] contexts) throws TransportException {
			throw new TransportException("BogusTransport");
		}
		
		public void sendStats(String programId, StatsMessage[] messages, Context[] contexts) throws TransportException {
			throw new TransportException("BogusTransport");
		}
		
		public void shutdown() throws TransportException {
			throw new TransportException("BogusTransport");
		}
	}
	
	public class TestErrorHandler implements ErrorHandler {
		public void handleError(String error) {
			
		}

		/**
		 * Writes MonDemand errors that have an associated exception to standard error.
		 */
		public void handleError(String error, Exception e) {
		}
	}
}
