package proj.zoie.mbean;

import java.util.Date;

import proj.zoie.impl.indexing.IndexUpdatedEvent;
import proj.zoie.impl.indexing.IndexingEventListener;
import proj.zoie.impl.indexing.ZoieSystem;

public class ZoieIndexingStatusAdmin implements ZoieIndexingStatusAdminMBean,IndexingEventListener{
	private final ZoieSystem<?,?> _zoieSystem;
	private long _endTime;
	private long _startTime;
	private int _leftOver;
	private int _size;
	private long _totalTime;
	private int _totalSize;
	
	public ZoieIndexingStatusAdmin(ZoieSystem<?,?> zoieSystem){
		_zoieSystem = zoieSystem;
		_zoieSystem.addIndexingEventListener(this);
		_startTime = 0L;
		_endTime = 0L;
		_leftOver = 0;
		_size = 0;
		_totalSize = 0;
		_totalTime = 0;
	}
	
	public long getAverageIndexingBatchDuration() {
		return _totalSize == 0 ? 0 : _totalTime/_totalSize;
	}

	public long getLastIndexingBatchDuration() {
		return _endTime - _startTime;
	}

	public int getLastIndexingBatchLeftOver() {
		return _leftOver;
	}

	public int getLastIndexingBatchSize() {
		return _size;
	}

	public Date getLastIndexingEndTime() {
		return new Date(_endTime);
	}

	public void resetAverage() {
		_totalSize = 0;
		_totalTime = 0;
	}

	public void handleIndexingEvent(IndexingEvent evt) {
		// only interested in IndexUpdateEvent
		if (evt instanceof IndexUpdatedEvent){
			IndexUpdatedEvent updateEvt = (IndexUpdatedEvent)evt;
			_startTime = updateEvt.getStartIndexingTime();
			_endTime = updateEvt.getEndIndexingTime();
			_leftOver = updateEvt.getNumDocsLeftInQueue();
			_size = updateEvt.getNumDocsIndexed();
			_totalSize += _size;
			_totalTime += (_endTime - _startTime);
		}	
	}
}
