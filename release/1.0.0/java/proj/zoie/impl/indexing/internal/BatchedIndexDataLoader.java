package proj.zoie.impl.indexing.internal;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;

public class BatchedIndexDataLoader<V> implements DataConsumer<V> {

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
	  
	  public static final int MAX_BATCH_SIZE=10000;
	  
	  private static Logger log = Logger.getLogger(BatchedIndexDataLoader.class);
	  
	  public BatchedIndexDataLoader(DataConsumer<V> dataLoader,int batchSize,int maxBatchSize,long delay)
	  {
	    _maxBatchSize=maxBatchSize;
	    _batchSize=batchSize;
	    _delay=delay;
	    _dataLoader=dataLoader;
	    _batchList=new LinkedList<DataEvent<V>>();
	    _lastFlushTime=0L;
	    _eventCount=0;
	    _loadMgrThread=new LoaderThread();
	    _loadMgrThread.setName("disk indexer data loader");
	    _stop=false;
	    _flush=false;
	  }
	  
	  public int getMaxBatchSize()
	  {
	    return _maxBatchSize;
	  }
	  
	  public void setMaxBatchSize(int maxBatchSize)
	  {
	    _maxBatchSize = maxBatchSize > 0 ? maxBatchSize : MAX_BATCH_SIZE;
	  }
	  
	  public int getBatchSize()
	  {
	    return _batchSize;
	  }
	  
	  public void setBatchSize(int batchSize)
	  {
	    _batchSize=Math.max(1, batchSize);
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
	  
	  public void consume(Collection<DataEvent<V>> events)
	  {
	    synchronized(this)
	    {
	      while (_batchList.size()>_maxBatchSize)
	      {
	        try
	        {
	        	BatchedIndexDataLoader.this.wait();
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

	  private static final Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler()
		{
		  public void uncaughtException(Thread thread, Throwable t)
		  {
		    log.error(thread.getName() + " is abruptly terminated" , t);
		  }
		};
		
	  private class LoaderThread extends Thread
	  {		  
	    LoaderThread()
	    {
	      super("disk indexer data loader");
	      this.setUncaughtExceptionHandler(exceptionHandler);
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
	            // do the batch
	            tmpList=_batchList;
	            _batchList=new LinkedList<DataEvent<V>>();
	          }
	        }
	        
	        if (tmpList != null)
	        {
	          long t1=System.currentTimeMillis();
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
	              _eventCount -= tmpList.size();
	              log.info(BatchedIndexDataLoader.this+"flushed batch of "+tmpList.size()+" events to disk indexer, took: "+(t2-t1)+" current event count: "+_eventCount);
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
	  }
}
