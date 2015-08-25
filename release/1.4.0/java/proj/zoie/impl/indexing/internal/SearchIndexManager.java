package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.ZoieMergePolicy;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.api.impl.util.IntSetAccelerator;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

public class SearchIndexManager<R extends IndexReader>{
    private static final Logger log = Logger.getLogger(SearchIndexManager.class);
    
	public static enum Status
	  {
	    Sleep, Working
	  }

	  private File _location;
	  private final	IndexReaderDecorator<R>	_indexReaderDecorator;
	  private final ZoieMergePolicy _mergePolicy;
	  private volatile DiskSearchIndex _diskIndex;
	  private volatile RAMSearchIndex _memIndexA;
	  private volatile RAMSearchIndex _memIndexB;
	  
	  private volatile Status _diskIndexerStatus;

	  private volatile RAMSearchIndex _currentWritable;
	  private volatile RAMSearchIndex _currentReadOnly;

	  // disk index reader
	  private volatile ZoieIndexReader _diskIndexReader;

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
	  
      public void setNumLargeSegments(int numLargeSegments)
      {
        _mergePolicy.setNumLargeSegments(numLargeSegments);
      }
      
      public int getNumLargeSegments()
      {
        return _mergePolicy.getNumLargeSegments();
      }
      
      public void setMaxSmallSegments(int maxSmallSegments)
      {
        _mergePolicy.setMaxSmallSegments(maxSmallSegments);
      }
      
      public int getMaxSmallSegments()
      {
        return _mergePolicy.getMaxSmallSegments();
      }
      
      public void setPartialExpunge(boolean doPartialExpunge)
      {
        _mergePolicy.setPartialExpunge(doPartialExpunge);
      }
      
      public boolean getPartialExpunge()
      {
        return _mergePolicy.getPartialExpunge();
      }
      
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
	  public Status getDiskIndexerStatus()
	  {
	    return _diskIndexerStatus;
	  }
	  
	  public R getDiskIndexReader() throws IOException
	  {
	    ZoieIndexReader reader = _diskIndexReader;
	    if (reader != null)
	    {
	      return (R)reader.getDecoratedReader();
	    }
	    else
	    {
	      return null;
	    }
	  }
	  
	  public List<R> getIndexReaders()
	      throws IOException
	  {
	    ArrayList<R> readers = new ArrayList<R>(3);
	    ZoieIndexReader reader = null;
	    
	    try
	    {
          IntSet memDelSet = null;
          IntSet diskDelSet = null;
          
	      _rwLock.readLock().lock();            // read lock imposed to ensure consistency
	      
          if (_memIndexB != null)                           // load memory index B
          {
            reader = _memIndexB.openIndexReader();            
            if (reader != null)
            {
              memDelSet = reader.getModifiedSet();
              if(memDelSet != null && memDelSet.size() > 0)
              {
                diskDelSet = new IntOpenHashSet(memDelSet);
              }
              readers.add((R)reader.getDecoratedReader());
            }
          }
          
          if (_memIndexA != null)                           // load memory index A
          {
            reader = _memIndexA.openIndexReader();
	        if (reader != null)
	        {
	          IntSet tmpDelSet = reader.getModifiedSet();
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
	          
	          reader.setDelSet(memDelSet != null ? new IntSetAccelerator(memDelSet) : null);
              readers.add((R)reader.getDecoratedReader());
	        }
	      }
	      
	      if (_diskIndex != null)                           // load disk index
	      {
	        reader = _diskIndexReader;
	        if (reader != null)
	        {
	          reader.setDelSet(diskDelSet != null ? new IntSetAccelerator(diskDelSet) : null);
	          readers.add((R)reader.getDecoratedReader());
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
	        
	        // no lock is needed, no new indexes are published
            _memIndexB = new RAMSearchIndex(version, _indexReaderDecorator);
            _currentWritable = _memIndexB;
            _currentReadOnly = _memIndexA;
	        log.info("Current writable index is B, new B created");
	      }
	      else
	      {
	        // from working to sleep
	        ZoieIndexReader diskIndexReader = null;
	        try
	        {
              // load a new reader, not in the lock because this should be done in the background
              // and should not contend with the readers
              diskIndexReader = _diskIndex.getNewReader();
	        }
	        catch (IOException e)
	        {
              log.error(e.getMessage(),e);
              try
              {
                if(diskIndexReader != null) diskIndexReader.close();
              }
              catch(Exception ignore)
              {
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
              _diskIndexReader = diskIndexReader;
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
	  private void init()
	  {
	    _diskIndexerStatus = Status.Sleep;
	    _diskIndex = new DiskSearchIndex(_location, _indexReaderDecorator, _mergePolicy); 
	    _diskIndexReader = null;
	    if(_diskIndex != null)
	    {
	      ZoieIndexReader diskIndexReader = null;
	      try
	      {
	        diskIndexReader = _diskIndex.getNewReader();
	      }
          catch (IOException e)
          {
            log.error(e.getMessage(),e);
            return;
          }
          _diskIndexReader = diskIndexReader;
	    }

	    long version = _diskIndex.getVersion();
	    
	    _memIndexA = new RAMSearchIndex(version, _indexReaderDecorator);
        _memIndexB = null;
        _currentWritable = _memIndexA;
        _currentReadOnly = null;
	  }

	  public BaseSearchIndex getDiskIndex()
	  {
	    return _diskIndex;
	  }

	  public RAMSearchIndex getCurrentWritableMemoryIndex()
	  {
	    return _currentWritable;
	  }
	  
	  public RAMSearchIndex getCurrentReadOnlyMemoryIndex()
	  {
	    return _currentReadOnly;
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
	  public void purgeIndex()
	  {
		log.info("purging index ...");
		
        FileUtil.rmDir(_location);
        
        if(_diskIndex != null)
		{
          _diskIndex.refresh();
          _diskIndexReader = null;		
		  _memIndexA = new RAMSearchIndex(_diskIndex.getVersion(), _indexReaderDecorator);
          _memIndexB = null;
		  _currentWritable = _memIndexA;
          _currentReadOnly = null;
		}
		
		log.info("index purged");
	  }
	  
	  public void refreshDiskReader() throws IOException
	  {
		  log.info("refreshing disk reader ...");
          ZoieIndexReader diskIndexReader = null;
          try
          {
            // load a new reader, not in the lock because this should be done in the background
            // and should not contend with the readers
            diskIndexReader = _diskIndex.getNewReader();
          }
          catch(IOException e)
          {
            log.error(e.getMessage(),e);
            if(diskIndexReader != null) diskIndexReader.close();
            throw e;
          }

          _diskIndexReader = diskIndexReader;

		  log.info("disk reader refreshed");
	  }
}
