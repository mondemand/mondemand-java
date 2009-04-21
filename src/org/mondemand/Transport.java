package org.mondemand;

public interface Transport {
	public void sendLogs(String programId, LogMessage[] messages, Context[] contexts) throws TransportException;
	public void sendStats(String programId, StatsMessage[] messages, Context[] contexts) throws TransportException;
	public void shutdown() throws TransportException;
}
