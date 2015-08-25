package proj.zoie.mbean;

public interface DataProviderAdminMBean {
	void start();
	void stop();
	void pause();
	void resume();
	void setBatchSize(int batchSize);
	int getBatchSize();
}
