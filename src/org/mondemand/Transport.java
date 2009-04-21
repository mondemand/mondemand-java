package org.mondemand;

public interface Transport {
	public void sendLogs(LogMessage[] messages);
	public void sendStats(StatsMessage[] messages);
	public void shutdown();
}
