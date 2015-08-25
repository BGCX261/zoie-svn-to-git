package proj.zoie.mbean;

import java.io.IOException;
import java.util.Date;

import proj.zoie.api.ZoieException;

public interface ZoieSystemAdminMBean {
	int getDiskIndexSize();
	
	long getCurrentDiskVersion() throws IOException;
	
	int getRamAIndexSize();
	
	long getRamAVersion();
	
	int getRamBIndexSize();
	
	long getRamBVersion();
	
	String getDiskIndexerStatus();
	
	long getBatchDelay();
	
	void setBatchDelay(long delay);
	
	int getBatchSize();
	
	void setBatchSize(int batchSize);
	
	boolean isRealtime();
	
	String getIndexDir();
	
	void refreshDiskReader()  throws IOException;
	
	Date getLastDiskIndexModifiedTime();
	  
	Date getLastOptimizationTime();
	  
	long getOptimizationDuration();
	  
	void setOptimizationDuration(long duration);
	  
	void optimize() throws IOException;
	
	void flushToDiskIndex(long timeout) throws ZoieException;
	
	void purgeIndex() throws IOException;

	int getMaxBatchSize();
	  
	void setMaxBatchSize(int maxBatchSize);  
}
