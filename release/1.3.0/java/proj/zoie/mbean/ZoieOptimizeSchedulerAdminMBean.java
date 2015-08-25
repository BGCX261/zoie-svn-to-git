package proj.zoie.mbean;

import java.util.Date;

public interface ZoieOptimizeSchedulerAdminMBean {
	long getOptimizationDuration();  
	void setOptimizationDuration(long duration);
	void setDateToStartOptimize(Date optimizeStartDate);
	Date getDateToStartOptimize();
    void setMaxSegments(int maxSegments);
	int getMaxSegments();
	void setMinSegments(int minSegments);
	int getMinSegments();
}
