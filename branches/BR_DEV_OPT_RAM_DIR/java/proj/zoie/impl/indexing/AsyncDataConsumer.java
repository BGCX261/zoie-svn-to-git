package proj.zoie.impl.indexing;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;

public class AsyncDataConsumer<V> implements DataConsumer<V>
{
  private static final Logger log = Logger.getLogger(AsyncDataConsumer.class);
  
  private ConsumerThread _consumerThread;
  private DataConsumer<V> _consumer;
  private long _currentVersion;
  private long _bufferedVersion;
  private LinkedList<DataEvent<V>> _batch;
  private int _batchSize;

  public AsyncDataConsumer()
  {
    _currentVersion = -1L;
    _bufferedVersion = -1L;
    _batch = new LinkedList<DataEvent<V>>();
    _batchSize = 1; // default
    _consumerThread = null;
  }
  
  public void start()
  {
    _consumerThread = new ConsumerThread();
    _consumerThread.setDaemon(true);
    _consumerThread.start();
  }
  
  public void stop()
  {
    _consumerThread.terminate();
  }
  
  public void setDataConsumer(DataConsumer<V> consumer)
  {
    synchronized(this)
    {
      _consumer = consumer;
    }
  }
  
  public void setBatchSize(int batchSize)
  {
    synchronized(this)
    {
      _batchSize = Math.max(1, batchSize);
    }
  }
  
  public int getBatchSize()
  {
    synchronized(this)
    {
      return _batchSize;
    }
  }
  
  public int getCurrentBatchSize()
  {
    synchronized(this)
    {
      return (_batch != null ? _batch.size() : 0);
    }
  }
  
  public long getCurrentVersion()
  {
    synchronized(this)
    {
      return _currentVersion;
    }
  }
  
  public void flushEvents(long timeout) throws ZoieException
  {
    syncWthVersion(timeout, _bufferedVersion);
  }
  
  public void syncWthVersion(long timeInMillis, long version) throws ZoieException
  {
    long now = System.currentTimeMillis();
    long due = now + timeInMillis;
    
    if(_consumerThread == null) throw new ZoieException("not running");
    
    synchronized(this)
    {
      while(_currentVersion < version)
      {
        if(now >= due)
        {
          throw new ZoieException("sync timed out");
        }
        try
        {
          this.wait(due - now);
        }
        catch(InterruptedException e)
        {
          log.warn(e.getMessage(), e);
        }
        now = System.currentTimeMillis();
      }
    }
  }
  
  public void consume(Collection<DataEvent<V>> data) throws ZoieException
  {
    if (data == null || data.size() == 0) return;
    
    synchronized(this)
    {
      while(_batch.size() >= _batchSize)
      {
        try
        {
          this.wait();
        }
        catch (InterruptedException e)
        {
        }
      }
      for(DataEvent<V> event : data)
      {
        _bufferedVersion = Math.max(_bufferedVersion, event.getVersion());
        _batch.add(event);
      }
      this.notifyAll(); // wake up the thread waiting in flushBuffer()
    }
  }
  
  protected final void flushBuffer()
  {
    long version;
    LinkedList<DataEvent<V>> currentBatch;
    
    synchronized(this)
    {
      while(_batch.size() == 0)
      {
        try
        {
          this.wait();
        }
        catch (InterruptedException e)
        {
        }
      }
      version = Math.max(_currentVersion, _bufferedVersion);
      currentBatch = _batch;
      _batch = new LinkedList<DataEvent<V>>();
      
      this.notifyAll(); // wake up the thread waiting in addEventToBuffer()
    }
    
    if(_consumer != null)
    {
      try
      {
        _consumer.consume(currentBatch);
      }
      catch (Exception e)
      {
        log.error(e.getMessage(), e);
      }
    }
    
    synchronized(this)
    {
      _currentVersion = version;
      this.notifyAll(); // wake up the thread waiting in syncWthVersion()
    }    
  }
  
  private final class ConsumerThread extends IndexingThread
  {
    boolean _stop = false;
    
    ConsumerThread()
    {
      super("ConsumerThread");
    }
    
    public void terminate()
    {
      _stop = true;
      synchronized(AsyncDataConsumer.this)
      {
    	  AsyncDataConsumer.this.notifyAll();
      }
    }
    
    public void run()
    {
      while(!_stop)
      {
        flushBuffer();
      }
    }
  }
}
