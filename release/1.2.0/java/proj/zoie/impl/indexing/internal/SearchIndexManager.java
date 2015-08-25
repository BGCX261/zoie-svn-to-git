package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class SearchIndexManager<R extends IndexReader>{
    private static final Logger log = Logger.getLogger(SearchIndexManager.class);
    
	public static enum Status
	  {
	    Sleep, Working
	  }

	  private File _location;
	  private final	IndexReaderDecorator<R>	_indexReaderDecorator;
	  private volatile DiskSearchIndex _diskIndex;
	  private volatile RAMSearchIndex _memIndexA;
	  private volatile RAMSearchIndex _memIndexB;
	  
	  private volatile Status _diskIndexerStatus;

	  private volatile RAMSearchIndex _currentWritable;

	  // disk index reader
	  // we need to cache both ZoieIndexReader version of disk index reader
	  // and its decorated version
	  private volatile ZoieIndexReader _diskIndexReader;
	  private volatile R _decoratedDiskIndexReader;

	  private final ReadWriteLock _rwLock;
	  
	  public SearchIndexManager(File location,IndexReaderDecorator<R> indexReaderDecorator)
	  {
	    _rwLock=new ReentrantReadWriteLock();
	    _location = location;
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
	  
	  public void setMergeFactor(int mergeFactor)
	  {
		_diskIndex.setMergeFactor(mergeFactor);
	  }
		
	  public int getMergeFactor()
	  {
		return _diskIndex.getMergeFactor();
	  }
		
	  public void setMaxMergeDocs(int maxMergeDocs)
	  {
		_diskIndex.setMaxMergeDocs(maxMergeDocs);
	  }
		
	  public int getMaxMergeDocs()
	  {
		return _diskIndex.getMaxMergeDocs();
	  }
	  
	  public void setUseCompoundFile(boolean useCompoundFile)
	  {
		_diskIndex.setUseCompoundFile(useCompoundFile);
	  }
	  
	  public boolean isUseCompoundFile()
	  {
	    return _diskIndex.isUseCompoundFile();
	  }
	  
	  /**
	   * Gets the current disk indexer status
	   * @return
	   */
	  public Status getDiskIndexerStatus()
	  {
	    return _diskIndexerStatus;
	  }
	  
	  public R getDiskIndexReader()
	  	  throws IOException
	  {
		  try
		  {
			  _rwLock.readLock().lock();
			  if (_diskIndex != null)                           // load disk index
		      {
		        return _decoratedDiskIndexReader;
		      }
			  else
			  {
				  return null;
			  }
		  }
		  finally
		  {
			  _rwLock.readLock().unlock();
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
                diskDelSet = new IntRBTreeSet(memDelSet);
              }
              
              readers.add(_indexReaderDecorator.decorate(reader));
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
	          
	          reader.setDelSet(memDelSet);
              readers.add(_indexReaderDecorator.decorate(reader));
	        }
	      }
	      
	      if (_diskIndex != null)                           // load disk index
	      {
	        reader = _diskIndexReader;
	        if (reader != null)
	        {
	          reader.setDelSet(diskDelSet);
	          readers.add(_decoratedDiskIndexReader);
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
	        _memIndexB = new RAMSearchIndex(version);
	        _currentWritable = _memIndexB;
	        log.info("Current writable index is B, new B created");
	      }
	      else
	      {
	        // from working to sleep
	        ZoieIndexReader diskIndexReader;
	        R decoratedReader;
	        try
	        {
              // load a new reader, not in the lock because this should be done in the background
              // and should not contend with the readers
              diskIndexReader = _diskIndex.getNewReader();
              decoratedReader = _indexReaderDecorator.decorate(diskIndexReader);
	        }
	        catch (IOException e)
	        {
	          log.error(e.getMessage(),e);
	          return;
	        }
	        try
	        {
	          _rwLock.writeLock().lock();
	          _memIndexA = _memIndexB;
	          _memIndexB = null;
	          _currentWritable = _memIndexA;
              _diskIndexReader = diskIndexReader;
              _decoratedDiskIndexReader = decoratedReader;
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
	    _diskIndex = new DiskSearchIndex(_location); 
	    if(_diskIndex != null)
	    {
	      try
	      {
	        _diskIndexReader = _diskIndex.getNewReader();
            _decoratedDiskIndexReader = _indexReaderDecorator.decorate(_diskIndexReader);
	      }
          catch (IOException e)
          {
            log.error(e.getMessage(),e);
            return;
          }
	    }

	    long version = _diskIndex.getVersion();
	    
	    _memIndexA = new RAMSearchIndex(version);
	    _currentWritable = _memIndexA;
	    _memIndexB = null;
	  }

	  public BaseSearchIndex getDiskIndex()
	  {
	    return _diskIndex;
	  }

	  public RAMSearchIndex getCurrentWritableMemoryIndex()
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
          ZoieIndexReader diskIndexReader;
          R decoratedReader;

          // load a new reader, not in the lock because this should be done in the background
          // and should not contend with the readers
          diskIndexReader = _diskIndex.getNewReader();
          decoratedReader = _indexReaderDecorator.decorate(diskIndexReader);

          try
          {
            _rwLock.writeLock().lock();
            _diskIndexReader = diskIndexReader;
            _decoratedDiskIndexReader = decoratedReader;
          }
          finally
          {
            _rwLock.writeLock().unlock();
          }

		  log.info("disk reader refreshed");
	  }
}
