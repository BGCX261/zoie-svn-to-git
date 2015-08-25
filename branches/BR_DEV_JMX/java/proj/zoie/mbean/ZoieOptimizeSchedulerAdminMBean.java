package proj.zoie.mbean;

import java.util.Date;

public interface ZoieOptimizeSchedulerAdminMBean {
	long getOptimizationDuration();  
	void setOptimizationDuration(long duration);
	void setDateToStartOptimize(Date optimizeStartDate);
	Date getDateToStartOptimize();
}
