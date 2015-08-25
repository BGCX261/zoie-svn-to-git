package proj.zoie.impl.indexing;

import proj.zoie.impl.indexing.IndexingEventListener.IndexingEvent;

public final class IndexUpdatedEvent extends IndexingEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final int _numDocsIndexed;
	private final long _startIndexingTime;
	private final long _endIndexingTime;
	private final int _numDocsLeftInQueue;
	
	public IndexUpdatedEvent(int numDocsIndexed,long startIndexingTime,long endIndexingTime,int numDocsLeftInQueue){
		_numDocsIndexed = numDocsIndexed;
		_startIndexingTime = startIndexingTime;
		_endIndexingTime = endIndexingTime;
		_numDocsLeftInQueue = numDocsLeftInQueue;
	}

	public int getNumDocsIndexed() {
		return _numDocsIndexed;
	}

	public long getStartIndexingTime() {
		return _startIndexingTime;
	}

	public long getEndIndexingTime() {
		return _endIndexingTime;
	}

	public int getNumDocsLeftInQueue() {
		return _numDocsLeftInQueue;
	}
}
