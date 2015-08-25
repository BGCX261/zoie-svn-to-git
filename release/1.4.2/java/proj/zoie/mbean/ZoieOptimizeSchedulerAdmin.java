package proj.zoie.mbean;

import java.util.Date;

import proj.zoie.api.indexing.DefaultOptimizeScheduler;

public class ZoieOptimizeSchedulerAdmin implements ZoieOptimizeSchedulerAdminMBean {
	private DefaultOptimizeScheduler _optimizeScheduler;
	
	public ZoieOptimizeSchedulerAdmin(DefaultOptimizeScheduler optimizeScheduler){
		_optimizeScheduler = optimizeScheduler;
	}
	
	public long getOptimizationDuration() {
		return _optimizeScheduler.getOptimizeDuration();
	}

	public void setOptimizationDuration(long duration) {
		_optimizeScheduler.setOptimizeDuration(duration);
	}
	

	public void setDateToStartOptimize(Date optimizeStartDate){
		_optimizeScheduler.setDateToStartOptimize(optimizeStartDate);
	}
	
	public Date getDateToStartOptimize(){
		return _optimizeScheduler.getDateToStartOptimize();
	}
}
