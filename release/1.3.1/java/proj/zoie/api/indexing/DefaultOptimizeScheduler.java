package proj.zoie.api.indexing;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;


public class DefaultOptimizeScheduler extends OptimizeScheduler {
	public static int DEFAULT_NUM_MIN_SEGS = 15;
	public static int DEFAULT_NUM_MAX_SEGS = 20;
	private static long DAY_IN_MILLIS = 1000L*60L*60L*24L;
	private static Logger logger = Logger.getLogger(DefaultOptimizeScheduler.class);
	
	private int _minSegs;
	private int _maxSegs;
	
	private long _optimizeDuration;
	
	private volatile boolean _fullOptimizeScheduled;
	
	private Timer _fullOptimizeTimer;
	private TimerTask _currentOptimizationTimerTask;
	private Date _dateToStartOptimize;
	
	private class OptimizeTimerTask extends TimerTask{

		@Override
		public void run() {
			logger.info("full optimize scheduled");
			_fullOptimizeScheduled = true;
		}	
	}
	
	// calculate to run next day at 1am
	private static Date calculateNextDay(){
		Calendar cal = Calendar.getInstance();
		int curHour = cal.get(Calendar.HOUR_OF_DAY);
		cal.set(Calendar.HOUR_OF_DAY, 1);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		if (curHour > 1){ 
		  cal.set(Calendar.DAY_OF_YEAR, cal.get(Calendar.DAY_OF_YEAR)+1);
		}
		return cal.getTime();
	}
	
	public DefaultOptimizeScheduler(){
		_minSegs = DEFAULT_NUM_MIN_SEGS;
		_maxSegs = DEFAULT_NUM_MAX_SEGS;
		_optimizeDuration = DAY_IN_MILLIS;
		_fullOptimizeScheduled = false;
		_dateToStartOptimize = calculateNextDay();
		_currentOptimizationTimerTask = new OptimizeTimerTask();
		_fullOptimizeTimer = new Timer("index optimization timer",true);
		_fullOptimizeTimer.scheduleAtFixedRate(_currentOptimizationTimerTask, _dateToStartOptimize, _optimizeDuration);
	}
	
	public long getOptimizeDuration(){
		return _optimizeDuration;
	}
	
	public synchronized void setOptimizeDuration(long optimizeDuration){
		if (_optimizeDuration!= optimizeDuration){
			_currentOptimizationTimerTask.cancel();
			_fullOptimizeTimer.purge();
			_currentOptimizationTimerTask = new OptimizeTimerTask();
			_fullOptimizeTimer.scheduleAtFixedRate(_currentOptimizationTimerTask, _dateToStartOptimize, optimizeDuration);
			_optimizeDuration = optimizeDuration;
		}
	}

	public int getMinSegs() {
		return _minSegs;
	}

	public void setMinSegs(int segs) {
		_minSegs = segs;
	}

	public int getMaxSegs() {
		return _maxSegs;
	}

	public void setMaxSegs(int segs) {
		_maxSegs = segs;
	}
	
	public synchronized void setDateToStartOptimize(Date optimizeStartDate){
		if (!_dateToStartOptimize.equals(optimizeStartDate)){
			_currentOptimizationTimerTask.cancel();
			_fullOptimizeTimer.purge();
			_currentOptimizationTimerTask = new OptimizeTimerTask();
			_fullOptimizeTimer.scheduleAtFixedRate(_currentOptimizationTimerTask, optimizeStartDate, _optimizeDuration);
			_dateToStartOptimize = optimizeStartDate;
		}
	}
	
	public Date getDateToStartOptimize(){
		return _dateToStartOptimize;
	}
	
	@Override
	public int numSegsScheduledForOptmization(int currentNumSegs,long lastTimeOptmized) {
		if (_fullOptimizeScheduled){
			logger.info("full optimize triggered.");
			_fullOptimizeScheduled = false;
			return 1;
		}
		else{
		  long now = System.currentTimeMillis();
		  int numSegs = 0;
		  if ((now-lastTimeOptmized) > _optimizeDuration || numSegs >= _maxSegs){
			numSegs = Math.min(_minSegs, _maxSegs);
          }
		  return numSegs;
		}
	}
	
	public static void main(String[] args) {
		System.out.println(calculateNextDay());
	}
}
