package proj.zoie.impl.indexing.internal;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.IndexingThread;

public class BatchedIndexDataLoader<R extends IndexReader,V> implements DataConsumer<V> {

	private int _batchSize;
	  private long _delay;
	  private DataConsumer<V> _dataLoader;
	  private List<DataEvent<V>> _batchList;
	  private LoaderThread _loadMgrThread;
	  private long _lastFlushTime;
	  private int _eventCount;
	  private int _maxBatchSize;
	  private boolean _stop;
	  private boolean _flush;
	  private SearchIndexManager _idxMgr;
	  
	  public static final int MAX_BATCH_SIZE=10000;
	  
	  private static Logger log = Logger.getLogger(BatchedIndexDataLoader.class);
	  
	  public BatchedIndexDataLoader(DataConsumer<V> dataLoader,int batchSize,int maxBatchSize,long delay,SearchIndexManager<R> idxMgr)
	  {
	    _maxBatchSize=Math.min(maxBatchSize, MAX_BATCH_SIZE);
	    _batchSize=Math.min(batchSize, _maxBatchSize);
	    _delay=delay;
	    _dataLoader=dataLoader;
	    _batchList=new LinkedList<DataEvent<V>>();
	    _lastFlushTime=0L;
	    _eventCount=0;
	    _loadMgrThread=new LoaderThread();
	    _loadMgrThread.setName("disk indexer data loader");
	    _stop=false;
	    _flush=false;
	    _idxMgr = idxMgr;
	  }
	  
	  public int getMaxBatchSize()
	  {
	    return _maxBatchSize;
	  }
	  
	  public void setMaxBatchSize(int maxBatchSize)
	  {
	    _maxBatchSize = Math.min((maxBatchSize > 0 ? maxBatchSize : MAX_BATCH_SIZE), MAX_BATCH_SIZE);
	    _batchSize = Math.min(_batchSize, _maxBatchSize);
	  }
	  
	  public int getBatchSize()
	  {
	    return _batchSize;
	  }
	  
	  public void setBatchSize(int batchSize)
	  {
	    _batchSize=Math.min(Math.max(1, batchSize), _batchSize);
	  }
	  
	  public long getDelay()
	  {
	    return _delay;
	  }
	  
	  public void setDelay(long delay)
	  {
	    _delay=delay;
	  }
	  
	  public synchronized int getEventCount()
	  {
	    return _eventCount;
	  }
	  
	  public void consume(Collection<DataEvent<V>> events) throws ZoieException
	  {
	    try
	    {
	      _idxMgr.queueingStart();
	      synchronized(this)
	      {
	        while (_batchList.size()>_maxBatchSize)
	        {
	          // check if load manager thread is alive
	          if(_loadMgrThread == null || !_loadMgrThread.isAlive())
	          {
	            throw new ZoieException("load manager has stopped");
	          }
	          
	          try
	          {
	            BatchedIndexDataLoader.this.wait(60000); // 1 min
	          }
	          catch (InterruptedException e)
	          {
	            continue;
	          }
	        }
	        _eventCount += events.size();
	        _batchList.addAll(events);
	        this.notifyAll();
	      }
	    }  
	    finally
	    {
	      _idxMgr.queueingEnd();
	    }
	  }
	  
      public int getCurrentBatchSize()
      {
        synchronized(this)
        {
          return (_batchList != null ? _batchList.size() : 0);
        }
      }
      
      protected List<DataEvent<V>> getBatchList()
	  {
        List<DataEvent<V>> tmpList=_batchList;
        _batchList=new LinkedList<DataEvent<V>>();
        return tmpList;
	  }
	  
	  public void flushEvents(long timeOut) throws ZoieException
	  {
	    long now = System.currentTimeMillis();
	    long due = now + timeOut;

	    synchronized(this)
	    {
	      while(_eventCount>0)
	      {
	        _flush=true;
	        this.notifyAll();

	        if (now > due)
	        {
	          log.error("sync timed out");
	          throw new ZoieException("timed out");          
	        }
	        try
	        {
	          this.wait(due - now);
	        }
	        catch (InterruptedException e)
	        {
	          throw new ZoieException(e.getMessage(),e);
	        }
	        
	        now = System.currentTimeMillis();
	      }
	    }
	  }

	  private class LoaderThread extends IndexingThread
	  {		  
	    LoaderThread()
	    {
	      super("disk indexer data loader");
	    }
	    
	    public void run()
	    {
	      while(!_stop)
	      {
	        List<DataEvent<V>> tmpList=null;
	        long now=System.currentTimeMillis();
	        long duration=now-_lastFlushTime;

	        synchronized(BatchedIndexDataLoader.this)
	        {
	          while(_batchList.size()<_batchSize && !_stop && !_flush && duration<_delay)
	          {
	            try
	            {
	            	BatchedIndexDataLoader.this.wait(_delay);
	            }
	            catch (InterruptedException e)
	            {
	              log.warn(e.getMessage());
	            }
	            now=System.currentTimeMillis();
	            duration=now-_lastFlushTime;
	          }
	          _flush=false;
	          _lastFlushTime=now;

	          if (_batchList.size()>0)
	          {
	            // get the batch
	            tmpList = getBatchList();
	          }
	        }
	        
	        if (tmpList != null)
	        {
	          long t1=System.currentTimeMillis();
	          int eventCount = tmpList.size();
	          try
	          {
	            _dataLoader.consume(tmpList);
	          }
	          catch (ZoieException e)
	          {
	            log.error(e.getMessage(),e);
	          }
	          finally
	          {
	            long t2=System.currentTimeMillis();
	            synchronized(BatchedIndexDataLoader.this)
	            {
	              _eventCount -= eventCount;
	              log.info(BatchedIndexDataLoader.this+" flushed batch of "+eventCount+" events to disk indexer, took: "+(t2-t1)+" current event count: "+_eventCount);
	              BatchedIndexDataLoader.this.notifyAll();
	            }
	          }
	        }
	        else
	        {
	          log.info("batch size is 0");
	        }
	      }
	    }
	  }
	  
	  public void start()
	  {
	    _loadMgrThread.setName(String.valueOf(this));
	    _loadMgrThread.start();
	  }

	  public void shutdown()
	  {
	    synchronized(this)
	    {
	      _stop = true;
	      this.notifyAll();
	    }
	    try 
	    {
			_loadMgrThread.join();
		} catch (InterruptedException e) {
			log.error(e.getMessage(),e);
		}
	  }
}
