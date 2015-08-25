package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.ZoieMergePolicy;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.api.impl.util.IntSetAccelerator;
import proj.zoie.api.indexing.IndexReaderDecorator;

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
	  
	  private volatile Status _diskIndexerStatus;
      private volatile Mem _mem;

	  
	  
	  /**
	   * @param location 
	   * @param indexReaderDecorator
	   */
	  public SearchIndexManager(File location,IndexReaderDecorator<R> indexReaderDecorator)
	  {
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
	    ZoieIndexReader reader = _mem.get_diskIndexReader();
	    if (reader != null)
	    {
          @SuppressWarnings("unchecked")
          R r = (R)reader.getDecoratedReader();
	      return r;
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
	    
        IntSet memDelSet = null;
        IntSet diskDelSet = null;
        Mem mem = _mem;
        RAMSearchIndex memIndexB = mem.get_memIndexB();
        RAMSearchIndex memIndexA = mem.get_memIndexA();
        if (memIndexB != null)                           // load memory index B
        {
          reader = memIndexB.openIndexReader();            
          if (reader != null)
          {
            memDelSet = reader.getModifiedSet();
            if(memDelSet != null && memDelSet.size() > 0)
            {
              diskDelSet = new IntOpenHashSet(memDelSet);
            }
            @SuppressWarnings("unchecked")
            R r = (R)reader.getDecoratedReader();
            readers.add(r);
          }
        }
        
        if (memIndexA != null)                           // load memory index A
        {
          reader = memIndexA.openIndexReader();
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
	        @SuppressWarnings("unchecked")
	        R r =(R)reader.getDecoratedReader(); 
            readers.add(r);
	      }
	    }
	    if (_diskIndex != null)                           // load disk index
	      {
	      reader = mem.get_diskIndexReader();
	      if (reader != null)
	      {
	        reader.setDelSet(diskDelSet != null ? new IntSetAccelerator(diskDelSet) : null);
            @SuppressWarnings("unchecked")
            R r = (R)reader.getDecoratedReader();
	        readers.add(r);
	      }
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
	        
            Mem oldMem = _mem;
            RAMSearchIndex memIndexB = new RAMSearchIndex(version, _indexReaderDecorator);
            Mem mem = new Mem(oldMem.get_memIndexA(), memIndexB, memIndexB, oldMem.get_memIndexA(), oldMem.get_diskIndexReader());
            _mem = mem;
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
	        Mem oldMem = _mem;
	        Mem mem = new Mem(oldMem.get_memIndexB(), null, oldMem.get_memIndexB(), null, diskIndexReader);
	        _mem = mem;
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
        ZoieIndexReader diskIndexReader = null;
	    if(_diskIndex != null)
	    {
	      try
	      {
	        diskIndexReader = _diskIndex.getNewReader();
	      }
          catch (IOException e)
          {
            log.error(e.getMessage(),e);
            return;
          }
	    }
	    long version = _diskIndex.getVersion();
        RAMSearchIndex memIndexA = new RAMSearchIndex(version, _indexReaderDecorator);
	    Mem mem = new Mem(memIndexA, null, memIndexA, null, diskIndexReader);
	    _mem = mem;
	  }

	  public BaseSearchIndex getDiskIndex()
	  {
	    return _diskIndex;
	  }

	  public RAMSearchIndex getCurrentWritableMemoryIndex()
	  {
	    return _mem.get_currentWritable();
	  }
	  
	  public RAMSearchIndex getCurrentReadOnlyMemoryIndex()
	  {
	    return _mem.get_currentReadOnly();
	  }
	  
	  /**
	   * Clean up
	   */
	  public void close(){
	    if (_diskIndex!=null)
	    {
	      _diskIndex.close();
	    }
	    Mem mem = _mem;
	    if (mem.get_memIndexA()!=null)
	    {
	      mem.get_memIndexA().close();
	    }
	    if (mem.get_memIndexB()!=null)
	    {
	      mem.get_memIndexB().close();
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
        RAMSearchIndex memIndexA = _mem.get_memIndexA();
	    return (memIndexA==null) ? 0 : memIndexA.getNumdocs();
	  }
	  
	  public long getRamAVersion()
	  {
        RAMSearchIndex memIndexA = _mem.get_memIndexA();
	    return (memIndexA==null) ? 0L : memIndexA.getVersion();
	  }
	  
	  public int getRamBIndexSize()
	  {
        RAMSearchIndex memIndexB = _mem.get_memIndexB();
	    return (memIndexB==null) ? 0 : memIndexB.getNumdocs();
	  }
	  
	  public long getRamBVersion()
	  {
	    RAMSearchIndex memIndexB = _mem.get_memIndexB();
	    return (memIndexB==null) ? 0L : memIndexB.getVersion();
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
          RAMSearchIndex memIndexA = new RAMSearchIndex(_diskIndex.getVersion(), _indexReaderDecorator);
          Mem mem = new Mem(memIndexA, null, memIndexA, null, null);
          _mem = mem;
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
          Mem oldMem = _mem;
          Mem mem = new Mem(oldMem.get_memIndexA(),
                            oldMem.get_memIndexB(),
                            oldMem.get_currentWritable(),
                            oldMem.get_currentReadOnly(),
                            diskIndexReader);
          _mem = mem;
		  log.info("disk reader refreshed");
	  }
	  
  private final class Mem
  {
    private final RAMSearchIndex _memIndexA;
    private final RAMSearchIndex _memIndexB;
    private final RAMSearchIndex _currentWritable;
    private final RAMSearchIndex _currentReadOnly;
    private final ZoieIndexReader _diskIndexReader;
    Mem(RAMSearchIndex a, RAMSearchIndex b, RAMSearchIndex w, RAMSearchIndex r, ZoieIndexReader d)
    {
      _memIndexA = a;
      _memIndexB = b;
      _currentWritable = w;
      _currentReadOnly = r;
      _diskIndexReader = d;
    }
    
    protected RAMSearchIndex get_memIndexA()
    {
      return _memIndexA;
    }

    protected RAMSearchIndex get_memIndexB()
    {
      return _memIndexB;
    }

    protected RAMSearchIndex get_currentWritable()
    {
      return _currentWritable;
    }

    protected RAMSearchIndex get_currentReadOnly()
    {
      return _currentReadOnly;
    }

    protected ZoieIndexReader get_diskIndexReader()
    {
      return _diskIndexReader;
    }
  }
}
