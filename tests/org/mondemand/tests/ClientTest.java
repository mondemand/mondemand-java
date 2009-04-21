package org.mondemand.tests;

import org.mondemand.Client;
import org.mondemand.Context;
import org.mondemand.Level;
import org.mondemand.LogMessage;
import org.mondemand.StatsMessage;
import org.mondemand.TraceId;
import org.mondemand.Transport;
import org.mondemand.transport.LWESTransport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Enumeration;

public class ClientTest {
	private static Client client = new Client("ClientTest");
	private static boolean transportIsSet = false;
	private static ClientTestTransport transport = null;
	
	@Before
	public void setUp() throws Exception {
		if(!transportIsSet) {
			transport = new ClientTestTransport();
			client.addTransport(transport);
			transportIsSet = true;
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testProgId() {
		client.setProgramId("test123");
		assert(client.getProgramId().equals("test123"));
	}
	
	@Test
	public void testImmediateSendLevel() {
		client.setImmediateSendLevel(Level.EMERG);
		assert(client.getImmediateSendLevel() == Level.EMERG);
	}
	
	@Test
	public void testNoSendLevel() {
		client.setNoSendLevel(Level.INFO);
		assert(client.getNoSendLevel() == Level.INFO);
		assert(client.levelIsEnabled(Level.WARNING, null) == true);
		assert(client.levelIsEnabled(Level.INFO, null) == false);
		assert(client.levelIsEnabled(Level.DEBUG, new TraceId(1)) == true);
	}
	
	@Test
	public void testSetContexts() {
		for(int i=0; i<1000; ++i) {
			client.addContext("key" + i, "value" + i);
		}
		assert(client.getContext("key999") != null);
		
		for(int i=0; i<1000; ++i) {
			client.removeContext("key" + i);
		}
		assert(client.getContext("key1") == null);
		
		for(int i=0; i<1000; ++i) {
			client.addContext("key" + i, "value" + i);
		}
		
		int count = 0;
		Enumeration<String> keys = client.getContextKeys();
		while(keys.hasMoreElements()) {
			count++;
			keys.nextElement();
		}
		assert(count == 1000);
		
		client.removeAllContexts();
		
		assert(client.getContext("key1") == null);
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
		assert(transport.stats.length == 3);
	}
	
	@Test
	public void testDecrement() {
		client.decrement();
		client.decrement(5);
		client.decrement("testDecrement");
		client.decrement("testDecrement2", 10);
		client.flushStats();
		assert(transport.stats.length == 3);
	}
	
	@Test
	public void testSetKey() {
		client.setKey("testSetKey", 123);
		client.setKey("testSetKeyLong", 123L);
		client.flushStats();
		assert(transport.stats.length == 2);
	}
	
	@Test
	public void testLogMessages() {
		client.emerg("emerg");
		client.alert("alert");
		client.crit("crit");
		client.error("error");
		client.warning("warning");
		client.notice("notice");
		client.info("info");
		client.debug("debug");
		client.flushLogs();
		assert(transport.logs.length == 8);
	}
	
	@Test
	public void testLogGeneric() {
		client.log("logGeneric", 123, Level.ALERT, null, "Test Message", null);
		client.flushLogs();
		assert(transport.logs[0].getFilename().equals("logGeneric"));
		assert(transport.logs[0].getLine() == 123);
		assert(transport.logs[0].getLevel() == Level.ALERT);
		assert(transport.logs[0].getMessage().equals("Test Message"));
	}
	
	@Test
	public void testLwesTransport() {
		try {
			InetAddress address = InetAddress.getLocalHost();
			Transport t = new LWESTransport(address, 9191, null);
			client.addTransport(t);
			for(int i=0; i<1000; ++i) {
				client.log("testLwesTransport", 123, Level.CRIT, null, "Test Message", null);
			}
			client.flushLogs();
		} catch(Exception e) {}
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
		
		public void sendLogs(String programId, LogMessage[] messages, Context[] contexts) {
			logs = new LogMessage[messages.length];
			for(int i=0; i<logs.length; ++i) {
				logs[i] = messages[i];
			}
		}
		
		public void sendStats(String programId, StatsMessage[] messages, Context[] contexts) {
			stats = new StatsMessage[messages.length];
			for(int i=0; i<stats.length; ++i) {
				stats[i] = messages[i];
			}
		}
		
		public void shutdown() {
			
		}
	}
}
