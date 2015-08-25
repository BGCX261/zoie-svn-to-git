/**
 * 
 */
package proj.zoie.impl.indexing.internal;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.Indexable;
import proj.zoie.impl.indexing.IndexingEventListener;

/**
 * @author ymatsuda
 *
 */
public class CopyingIndexDataLoader<R extends IndexReader> extends BatchedIndexDataLoader<R,Indexable>
{
  private LuceneIndexDataLoader<R> _luceneDataLoader;
  private int _currentBatchSize;
  
  public static final int MAX_BATCH_SIZE = 10000;
  
  private static Logger log = Logger.getLogger(CopyingIndexDataLoader.class);
  
  public CopyingIndexDataLoader(LuceneIndexDataLoader<R> dataLoader,int batchSize,int maxBatchSize,long delay,SearchIndexManager<R> idxMgr,List<IndexingEventListener> lsnrList)
  {
    super(dataLoader, batchSize, maxBatchSize, delay, idxMgr,lsnrList);
    _luceneDataLoader = dataLoader;
    _currentBatchSize = 0;
    _idxMgr.queueingEnd(); // never call queueingStart, we want RT to be always ON.    
  }
  
  public void consume(Collection<DataEvent<Indexable>> events) throws ZoieException
  {
    synchronized(this)
    {
      while (_currentBatchSize > _maxBatchSize)
      {
        // check if load manager thread is alive
        if(_loadMgrThread == null || !_loadMgrThread.isAlive())
        {
          throw new ZoieException("load manager has stopped");
        }
        
        try
        {
          this.wait(60000); // 1 min
        }
        catch (InterruptedException e)
        {
          continue;
        }
      }
      int size = events.size();
      _eventCount += size;
      _currentBatchSize += size;
      this.notifyAll();
    }
  }
  
  public synchronized int getCurrentBatchSize()
  {
    return _currentBatchSize;
  }

  protected void processBatch()
  {
    RAMSearchIndex<R> readOnlyMemIndex = null;
    long now = System.currentTimeMillis();
    long duration = now - _lastFlushTime;
    int eventCount = 0;

    synchronized(this)
    {
      while(_currentBatchSize < _batchSize && !_stop && !_flush && duration < _delay)
      {
        try
        {
          wait(_delay - duration);
        }
        catch (InterruptedException e)
        {
          log.warn(e.getMessage());
        }
        now = System.currentTimeMillis();
        duration = now - _lastFlushTime;
      }
      _flush = false;
      _lastFlushTime = now;

      if (_currentBatchSize > 0)
      {
        // change the status and get the read only memory index
        // this has to be done in the block synchronized on CopyingBatchIndexDataLoader
        _idxMgr.setDiskIndexerStatus(SearchIndexManager.Status.Working);
        readOnlyMemIndex = _idxMgr.getCurrentReadOnlyMemoryIndex();
        eventCount = _currentBatchSize;
        _currentBatchSize = 0;
      }
    }
    
    if (readOnlyMemIndex != null)
    {
      long t1=System.currentTimeMillis();
      try
      {
        _luceneDataLoader.loadFromIndex(readOnlyMemIndex);
      }
      catch (ZoieException e)
      {
        log.error(e.getMessage(),e);
      }
      finally
      {
        long t2=System.currentTimeMillis();
        synchronized(this)
        {
          _eventCount -= eventCount;
          log.info(this+" flushed batch of "+eventCount+" events to disk indexer, took: "+(t2-t1)+" current event count: "+_eventCount);
          notifyAll();
        }
      }
    }
    else
    {
      log.info("batch size is 0");
    }
  }
}
