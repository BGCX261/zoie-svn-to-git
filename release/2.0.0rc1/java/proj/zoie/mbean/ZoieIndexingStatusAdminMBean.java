package proj.zoie.mbean;

import java.util.Date;

public interface ZoieIndexingStatusAdminMBean {
	long getLastIndexingBatchDuration();
	long getAverageIndexingBatchDuration();
	void resetAverage();
	Date getLastIndexingEndTime();
	int getLastIndexingBatchSize();
	int getLastIndexingBatchLeftOver();
}
