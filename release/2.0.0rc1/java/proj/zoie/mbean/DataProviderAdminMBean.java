package proj.zoie.mbean;

public interface DataProviderAdminMBean {
	void start();
	void stop();
	void pause();
	void resume();
	void setBatchSize(int batchSize);
    void setMaxEventsPerMinute(long maxEventsPerMinute);
	int getBatchSize();
	long getEventCount();
	long getEventsPerMinute();
	long getMaxEventsPerMinute();
    public String getStatus();
}
