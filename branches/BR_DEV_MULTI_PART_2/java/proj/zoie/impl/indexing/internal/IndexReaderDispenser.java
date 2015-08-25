package proj.zoie.impl.indexing.internal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ZoieInternalIndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.internal.ReaderDirectory.ReaderEntry;

public class IndexReaderDispenser<R extends IndexReader> {
	private static final Logger log = Logger.getLogger(IndexReaderDispenser.class);
	
	private static final int INDEX_OPEN_NUM_RETRIES=5;
	
	public static final String  INDEX_DIRECTORY = "index.directory";
	public static final String   INDEX_DIR_NAME = "beef";

	
	/**
	   * Gets the current signature
	   * @param indexHome
	   * @return
	   */
	public static IndexSignature getCurrentIndexSignature(File indexHome)
	{
	    File directoryFile = new File(indexHome, INDEX_DIRECTORY);
	    IndexSignature sig=IndexSignature.read(directoryFile);
	    return sig;
	}

	private final IndexReaderDecorator<R> _decorator;
	private volatile ReaderDirectory<R> _oldReaderDirectory;
	private volatile ReaderDirectory<R> _currentReaderDirectory;
	private final File _indexHome;
	
	public IndexReaderDispenser(File indexHome, IndexReaderDecorator<R> decorator)
	{
	  _indexHome = indexHome;
	  _decorator = decorator;
	  try
	  {
	    getNewReaderDirectory();
	  }
	  catch (IOException e)
	  {
	    log.error(e);
	  }
	}
	
	public int numDocs(){
		return _currentReaderDirectory == null ? 0 : _currentReaderDirectory.getNumDocs();
	}
	
	public long getCurrentVersion()
	{
		long version = 0L;
		if (_currentReaderDirectory!=null){
			IndexSignature sig = _currentReaderDirectory.getSignature();
			if (sig!=null){
				version = sig.getVersion();
			}
		}
		return version;
	}
	
	/**
	   * constructs a new IndexReader instance
	   * 
	   * @param indexPath
	   *            Where the index is.
	   * @return Constructed IndexReader instance.
	   * @throws IOException
	   */
	  private static <R extends IndexReader> ReaderDirectory<R> newReader(File luceneDir, 
			  IndexReaderDecorator<R> decorator, 
			  IndexSignature signature,
			  ReaderDirectory<R> oldDirectory)
	      throws IOException
	  {
	    if (!luceneDir.exists() || !IndexReader.indexExists(luceneDir))
	      return null;
	    Directory dir=FSDirectory.getDirectory(luceneDir);
	    int numTries=INDEX_OPEN_NUM_RETRIES;
	    ReaderDirectory<R> readerDir=null;
	    
	    // try max of 5 times, there might be a case where the segment file is being updated
	    while(readerDir==null)
	    {
	      if (numTries==0)
	      {
	    	  log.error("Problem refreshing disk index, all attempts failed.");
	        throw new IOException("problem opening new index");
	      }
	      numTries--;
	      
	      try{
	    	log.debug("opening index reader at: "+luceneDir.getAbsolutePath());
	        IndexReader srcReader = IndexReader.open(dir,true);
	        
	        try
	        {
	          readerDir = ZoieInternalIndexReader.buildIndexReaders(srcReader, decorator, signature, oldDirectory);
	        }
	        catch(IOException ioe)
	        {
	          // close the source reader if InternalIndexReader construction fails
	          if (srcReader!=null)
	          {
	            srcReader.close();
	          }
	          throw ioe;
	        }
	      }
	      catch(IOException ioe)
	      {
	        try
	        {
	          Thread.sleep(100);
	        }
	        catch (InterruptedException e)
	        {
	          log.warn("thread interrupted.");
	          continue;
	        }
	      }
	    }
	    return readerDir;
	  }
	  
	  public File getLuceneDir() throws IOException{
		  IndexSignature sig = getCurrentIndexSignature(_indexHome);
          if (sig==null)
          {
        	  return null;
          }
          else{
        	  return getLuceneDir(sig);
          }
	  }
	  
	  public File getLuceneDir(IndexSignature sig) throws IOException{
          String luceneDir = sig.getIndexPath();
    
          if (luceneDir == null || luceneDir.trim().length() == 0)
          {
            throw new IOException(INDEX_DIRECTORY + " contains no data.");
          }
          
          return new File(_indexHome,luceneDir);
	  }

	  /**
	   * get a fresh new reader instance
	   * @return an IndexReader instance, can be null if index does not yet exit
	   * @throws IOException
	   */
	  public ReaderDirectory<R> getNewReaderDirectory() throws IOException
	  {
	      // wack the old reader
	      if (_oldReaderDirectory != null)
	      {
	    	_oldReaderDirectory.dispose();
	        _oldReaderDirectory=null;
	      }

	      int numTries=INDEX_OPEN_NUM_RETRIES;   
	      ReaderDirectory<R> readerDirectory=null;
	      
	      // try it for a few times, there is a case where lucene is swapping the segment file, 
	      // or a case where the index directory file is updated, both are legitimate,
	      // trying again does not block searchers,
	      // the extra time it takes to get the reader, and to sync the index, memory index is collecting docs
	     
	      while(readerDirectory==null)
	      {
	        if (numTries==0)
	        {
	        	break;
	        }
	        numTries--;
	        try{
	          IndexSignature sig = getCurrentIndexSignature(_indexHome);
	          if (sig==null)
	          {
	            throw new IOException("no index exist");
	          }
	          
	          File luceneDir = getLuceneDir(sig);
	          readerDirectory = newReader(luceneDir, _decorator, sig, _currentReaderDirectory);
	          break;
	          
	        }
	        catch(IOException ioe)
	        {
	          try
	          {
	            Thread.sleep(100);
	          }
	          catch (InterruptedException e)
	          {
	        	log.warn("thread interrupted.");
	            continue;
	          }
	        }
	      }
	      _oldReaderDirectory = _currentReaderDirectory;
	      _currentReaderDirectory = readerDirectory;
	      
	      return _currentReaderDirectory;
	    }
	
	public ReaderDirectory<R> getCurrentReaderDirectory(){
		return _currentReaderDirectory;
	}
	
	public static <R extends IndexReader> Collection<R> extractIndexReaders(ReaderDirectory<R> readerDir){
		ReaderDirectory<R> tmpReader = readerDir;
		if (tmpReader!=null){
			Collection<ReaderEntry<R>> readerEntires = tmpReader.getIndexReaderEntries();
			ArrayList<R> list = new ArrayList<R>(readerEntires.size());
			for (ReaderEntry<R> entry : readerEntires){
				list.add(entry.decorated);
			}
			return list;
		}
		return new ArrayList<R>(0);
	}
	
	public Collection<R> getIndexReaders(){
		return extractIndexReaders(_currentReaderDirectory);
	}
	
	/**
	   * Closes the factory.
	   * 
	   */
	  public void close()
	  {
		ReaderDirectory<R> tmpReader = null;
	    tmpReader = _oldReaderDirectory;

	    _oldReaderDirectory = null;

	    try
	    {
	      // close the old reader
	      if (tmpReader != null)
	      {
	    	tmpReader.dispose();
	      }
	    }
	    finally
	    {
	      // close the current reader
	      tmpReader = _currentReaderDirectory;
	      _currentReaderDirectory = null;

	      if (tmpReader != null)
	      {
	    	tmpReader.dispose();
	      }
	    }
	  }

	  protected void finalize()
	  {
	    close();
	  }
}
