package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.ZoieMergePolicy;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.impl.util.IntSetAccelerator;
import proj.zoie.api.indexing.DefaultOptimizeScheduler;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.internal.ReaderDirectory.ReaderEntry;

public class SearchIndexManager<R extends IndexReader>{
    private static final Logger log = Logger.getLogger(SearchIndexManager.class);
    
	public static enum Status
	  {
	    Sleep, Working
	  }

	// realtime indexing control
	private AtomicLong _queueingStart = new AtomicLong(-1L);
	private long _queueingWait = 0; // Exponential Moving Average (EMA) of queueing wait time
    private long _rtThreshold = Long.MAX_VALUE;
    private boolean _rtSuspended = false;
    
	  private File _location;
	  private final	IndexReaderDecorator<R>	_indexReaderDecorator;
	  private final ZoieMergePolicy _mergePolicy;
	  private volatile DiskSearchIndex<R> _diskIndex;
	  private volatile RAMSearchIndex<R> _memIndexA;
	  private volatile RAMSearchIndex<R> _memIndexB;
	  
	  private volatile Status _diskIndexerStatus;

	  private volatile RAMSearchIndex<R> _currentWritable;
	  private volatile RAMSearchIndex<R> _currentReadOnly;

	  // disk index reader collection
	  private volatile ReaderDirectory<R> _diskReaderDirectory;

	  private final ReadWriteLock _rwLock;
	  
	  public SearchIndexManager(File location,IndexReaderDecorator<R> indexReaderDecorator)
	  {
	    _rwLock=new ReentrantReadWriteLock();
	    _location = location;
	    
	    _mergePolicy = new ZoieMergePolicy();
	    _mergePolicy.setUseCompoundFile(true);
	    _mergePolicy.setMergeFactor(LogMergePolicy.DEFAULT_MERGE_FACTOR);
	    _mergePolicy.setMaxMergeDocs(LogMergePolicy.DEFAULT_MAX_MERGE_DOCS);
	    
	    if (indexReaderDecorator!=null)
	    {
	      _indexReaderDecorator=indexReaderDecorator;
	    }
	    else
	    {
	      throw new IllegalArgumentException("indexReaderDecorator cannot be null");
	    }
	    init();
	  }
	  
	  public File getDiskIndexLocation()
	  {
	    return _location;
	  }
	  
	  /*
      public void setNumLargeSegments(int numLargeSegments)
      {
        _mergePolicy.setNumLargeSegments(numLargeSegments);
      }
      
      public int getNumLargeSegments()
      {
        return _mergePolicy.getNumLargeSegments();
      }
      
      public void setPartialExpunge(boolean doPartialExpunge)
      {
        _mergePolicy.setPartialExpunge(doPartialExpunge);
      }
      
      public boolean getPartialExpunge()
      {
        return _mergePolicy.getPartialExpunge();
      }*/
      
	  public void setMergeFactor(int mergeFactor)
	  {
	    _mergePolicy.setMergeFactor(mergeFactor);
	  }
	  
	  public int getMergeFactor()
	  {
		return _mergePolicy.getMergeFactor();
	  }
		
	  public void setMaxMergeDocs(int maxMergeDocs)
	  {
		_mergePolicy.setMaxMergeDocs(maxMergeDocs);
	  }
		
	  public int getMaxMergeDocs()
	  {
		return _mergePolicy.getMaxMergeDocs();
	  }
	  
	  public void setUseCompoundFile(boolean useCompoundFile)
	  {
		_mergePolicy.setUseCompoundFile(useCompoundFile);
	  }
	  
	  public boolean isUseCompoundFile()
	  {
	    return _mergePolicy.getUseCompoundFile();
	  }
	  
	  /**
	   * Gets the current disk indexer status
	   * @return
	   */
	  public Status getDiskIndexerStatus(){
	    return _diskIndexerStatus;
	  }
	  
	 /* public Collection<R> getDiskIndexReaderCollection()
	  	  throws IOException{
		  return _diskReaderCollection;
	  }
	  */
	  public List<R> getIndexReaders()
	      throws IOException
	  {
	    ArrayList<R> readers = new ArrayList<R>(DefaultOptimizeScheduler.DEFAULT_NUM_MAX_SEGS+5);
	    
	    try
	    {
          IntSet memDelSet = null;
          IntSet diskDelSet = null;
          
	      _rwLock.readLock().lock();            // read lock imposed to ensure consistency
	      ReaderEntry<R> readerEntry = null;
          if (_memIndexB != null)                           // load memory index B
          {
        	readerEntry = _memIndexB.getCunrrentReaderEntry();
            if (readerEntry != null)
            {
              memDelSet = readerEntry.undecorated.getModifiedSet();
              if(memDelSet != null && memDelSet.size() > 0)
              {
                diskDelSet = new IntOpenHashSet(memDelSet);
              }
              readers.add(readerEntry.decorated);
            }
          }
          
          if (_memIndexA != null)                           // load memory index A
          {
        	readerEntry = _memIndexA.getCunrrentReaderEntry();
	        if (readerEntry != null)
	        {
	          IntSet tmpDelSet = readerEntry.undecorated.getModifiedSet();
	          if(diskDelSet == null)
	          {
                diskDelSet = tmpDelSet;
	          }
	          else
	          {
                if(tmpDelSet != null && tmpDelSet.size() > 0)
                {
                  diskDelSet.addAll(tmpDelSet);
                }
	          }
	          
	          readerEntry.undecorated.setDelSet(memDelSet != null ? new IntSetAccelerator(memDelSet) : null);
              readers.add(readerEntry.decorated);
	        }
	      }
	      
	      if (_diskIndex != null)                           // load disk index
	      {
	          ReaderDirectory<R> readerDir = _diskReaderDirectory;
	          if (readerDir != null){
	            readerDir.setDeleteSet(diskDelSet != null ? new IntSetAccelerator(diskDelSet) : null);
		        readers.addAll(readerDir.getReaderCollection());
	          }
	        
	      }
	    }
	    finally
	    {
	      _rwLock.readLock().unlock();
	    }
	    return readers;
	  }
	  
	  public void setDiskIndexerStatus(Status status)
	  {
	    
	    // going from sleep to wake, disk index starts to index
	    // which according to the spec, index B is created and it starts to collect data
	    // IMPORTANT: do nothing if the status is not being changed.
	    if (_diskIndexerStatus != status)
	    {

	      log.info("updating batch indexer status from "+_diskIndexerStatus+" to "+status);
	      
	      if (status == Status.Working)
	      { // sleeping to working
	        long version = _diskIndex.getVersion();
	        
	        long queueingStart = _queueingStart.get();
	        long currentWait = (queueingStart > 0 ? System.currentTimeMillis() - queueingStart : 0);
            _queueingWait = (currentWait * 2 + _queueingWait)/3;
            
            RAMSearchIndex<R> newIndex;
            if(_queueingWait < _rtThreshold)
            {
              newIndex = new RAMSearchIndex<R>(version, _indexReaderDecorator);              
              
              if(_rtSuspended)
              {
                _rtSuspended = false;
                log.info("realtime indexing is resumed: queueing wait moving average "+ _queueingWait + "ms");
              }
            }
            else
            {
              // DISABLE REALTIME
              // we cannot do realtime indexing because we are waiting too long in queueing.
              // turn off realtime indexing until indexing load gets lighter.
              newIndex = new RAMSearchIndex<R>(version, _indexReaderDecorator)
              {
                public void updateIndex(IntSet delDocs, List<Document> insertDocs,Analyzer analyzer,Similarity similarity)
                {
                  // no indexing
                }
              };
              
              if(!_rtSuspended)
              {
                _rtSuspended = true;
                log.info("realtime indexing is suspended: queueing wait moving average "+ _queueingWait + "ms");
              }
            }
            
	        // no lock is needed, no new indexes are published
            _memIndexB = newIndex;
            _currentWritable = _memIndexB;
            _currentReadOnly = _memIndexA;
	        log.info("Current writable index is B, new B created");
	      }
	      else
	      {
	        // from working to sleep
	    	ReaderDirectory<R> newDirectory = null;
	        try
	        {
              // load a new reader, not in the lock because this should be done in the background
              // and should not contend with the readers
	        	newDirectory = _diskIndex.getNewReaderDirectory();
	        }
	        catch (IOException e)
	        {
              log.error(e.getMessage(),e);
              try
              {
                if(_diskReaderDirectory != null) _diskReaderDirectory.dispose();
              }
              catch(Exception ignore)
              {
            	  log.error(ignore.getMessage(),ignore);
              }
	          return;
	        }
	        try
	        {
	          _rwLock.writeLock().lock();
	          _memIndexA = _memIndexB;
	          _memIndexB = null;
              _currentWritable = _memIndexA;
              _currentReadOnly = null;
              _diskReaderDirectory = newDirectory;
	        }
	        finally
	        {
	          _rwLock.writeLock().unlock();
	        }
	        log.info("Current writable index is A, B is flushed");
	      }
	      _diskIndexerStatus = status;
	    }
	  }

	  /**
	   * Initialization
	   */
	  private void init(){
	    _diskIndexerStatus = Status.Sleep;
	    _diskIndex = new DiskSearchIndex<R>(_location, _indexReaderDecorator, _mergePolicy); 
	    if(_diskIndex != null){
          _diskReaderDirectory = _diskIndex.getCurrentReaderDirectory();
	    }

	    long version = _diskIndex.getVersion();
	    
	    _memIndexA = new RAMSearchIndex<R>(version, _indexReaderDecorator);
	    _memIndexB = null;
        _currentWritable = _memIndexA;
        _currentReadOnly = null;
	  }

	  public BaseSearchIndex<R> getDiskIndex()
	  {
	    return _diskIndex;
	  }

	  public RAMSearchIndex<R> getCurrentWritableMemoryIndex()
	  {
	    try
	    {
	      _rwLock.readLock().lock();
	      return _currentWritable;
	    }
	    finally
	    {
	      _rwLock.readLock().unlock();
	    }
	  }
	  
	  public RAMSearchIndex<R> getCurrentReadOnlyMemoryIndex()
	  {
	    try
	    {
	      _rwLock.readLock().lock();
	      return _currentReadOnly;
	    }
	    finally
	    {
	      _rwLock.readLock().unlock();
	    }
	  }
	  
	  /**
	   * Clean up
	   */
	  public void close(){
	    if (_diskIndex!=null)
	    {
	      _diskIndex.close();
	    }
	    if (_memIndexA!=null)
	    {
	      _memIndexA.close();
	    }
	    if (_memIndexB!=null)
	    {
	      _memIndexB.close();
	    }
	  }

	  
	  public long getCurrentDiskVersion() throws IOException
	  {
	    return (_diskIndex==null) ? 0 : _diskIndex.getVersion();
	  }

	  public int getDiskIndexSize()
	  {
	    return (_diskIndex==null) ? 0 : _diskIndex.getNumdocs();
	  }
	  
	  public int getRamAIndexSize()
	  {
	    return (_memIndexA==null) ? 0 : _memIndexA.getNumdocs();
	  }
	  
	  public long getRamAVersion()
	  {
	    return (_memIndexA==null) ? 0L : _memIndexA.getVersion();
	  }
	  
	  public int getRamBIndexSize()
	  {
	    return (_memIndexB==null) ? 0 : _memIndexB.getNumdocs();
	  }
	  
	  public long getRamBVersion()
	  {
	    return (_memIndexB==null) ? 0L : _memIndexB.getVersion();
	  }
	  
	  /**
	   * utility method to delete a directory
	   * @param dir
	   * @throws IOException
	   */
	  private static void deleteDir(File dir) throws IOException
	  {
	    if (dir == null) return;
	    
	    if (dir.isDirectory())
	    {
	      File[] files=dir.listFiles();
	      for (File file : files)
	      {
	        deleteDir(file);
	      }
	      if (!dir.delete())
	      {
	        throw new IOException("cannot remove directory: "+dir.getAbsolutePath());
	      }
	    }
	    else
	    {
	      if (!dir.delete())
	      {
	        throw new IOException("cannot delete file: "+dir.getAbsolutePath());
	      }
	    }
	  }

	  /**
	   * Purges an index
	   */
	  public void purgeDocs()
	  {
		log.info("purging index ...");
	    String name=_location.getName()+"-"+System.currentTimeMillis();
	    File parent=_location.getParentFile();
	    File tobeDeleted=new File(parent,name);
	    _location.renameTo(tobeDeleted);
	    init();
	    // try to delete the files, ok if it fails, this is just for testing
	    try{
	      deleteDir(tobeDeleted);
	    }
	    catch(IOException ioe)
	    {
	      log.warn("purged index could not remove all files");
	    }
		log.info("index purged");
	  }
	  
	  public void refreshDiskReader() throws IOException
	  {
		  log.info("refreshing disk reader ...");
          try
          {
            _rwLock.writeLock().lock();
            _diskReaderDirectory = _diskIndex.getCurrentReaderDirectory();
          }
          finally
          {
            _rwLock.writeLock().unlock();
          }

		  log.info("disk reader refreshed");
	  }

	  public void setRealtimeThreshold(long rtThreshold)
	  {
	    _rtThreshold = rtThreshold;
	  }
	  
	  public long getRealtimeThreshold()
	  {
	    return _rtThreshold;
	  }
	  
	  public boolean isRealtimeSuspended()
	  {
	    return _rtSuspended;
	  }
	  
      public void queueingStart()
      {
        _queueingStart.set(System.currentTimeMillis());
      }
      
      public void queueingEnd()
      {
        _queueingStart.set(-1);
      }
}
