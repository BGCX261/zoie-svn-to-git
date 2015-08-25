package proj.zoie.impl.indexing.internal;

import java.io.File;
import java.io.IOException;
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
	private volatile ReaderDirectory<R> _currentReaderDirectory;
    private volatile IndexReader _currentReader;
    private volatile ReaderEntry<R> _diskReaderEntry;
	private final File _indexHome;
	
	public IndexReaderDispenser(File indexHome, IndexReaderDecorator<R> decorator)
	{
	  _indexHome = indexHome;
	  _decorator = decorator;
	  
	  IndexSignature sig = getCurrentIndexSignature(_indexHome);
	  if(sig != null)
	  {
	    try
	    {
	      getNewReaderDirectory();
	    }
	    catch (IOException e)
	    {
	      log.error(e);
	    }
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
	  private IndexReader newReader(File luceneDir)
	      throws IOException
	  {
	    if (!luceneDir.exists() || !IndexReader.indexExists(luceneDir))
	      return null;
	    Directory dir=FSDirectory.getDirectory(luceneDir);
	    int numTries=INDEX_OPEN_NUM_RETRIES;
	    IndexReader srcReader = null;
	    
	    // try max of 5 times, there might be a case where the segment file is being updated
	    while(srcReader == null)
	    {
	      if (numTries==0)
	      {
	    	  log.error("Problem refreshing disk index, all attempts failed.");
	        throw new IOException("problem opening new index");
	      }
	      numTries--;
	      
	      try
	      {
	    	log.debug("opening index reader at: "+luceneDir.getAbsolutePath());
	    	if(_currentReader == null)
	    	{
	    	  srcReader = IndexReader.open(dir,true);
	    	}
	    	else
	    	{
	    	  srcReader = _currentReader.reopen();
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
	    return srcReader;
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
	   * get a new reader directory instance
	   * @return a ReaderDirectory instance, can be null if index does not yet exit
	   * @throws IOException
	   */
	  public ReaderDirectory<R> getNewReaderDirectory() throws IOException
	  {
	    ReaderDirectory<R> readerDirectory = null;
	    IndexReader srcReader = null;
	    
	    IndexSignature sig = getCurrentIndexSignature(_indexHome);
	    if(sig == null)
	    {
	      throw new IOException("no index exist");
	    }
	    
	    File luceneDir = getLuceneDir(sig);
	    srcReader = newReader(luceneDir);
	    if(srcReader != null)
	    {
	      try
	      {
	        readerDirectory = ZoieInternalIndexReader.buildReaderDirectory(srcReader, _decorator, sig, _currentReaderDirectory);
	      }
	      catch(IOException ioe)
	      {
	        if (srcReader != null)
	        {
	          srcReader.close();
	        }
	        throw ioe;
	      }
	    }
	    _currentReaderDirectory = readerDirectory;
	    _currentReader = srcReader;
	    _diskReaderEntry = null;
	    
	    return _currentReaderDirectory;
	  }
	
	public ReaderDirectory<R> getCurrentReaderDirectory(){
		return _currentReaderDirectory;
	}
	
	public R getDiskIndexReader() throws IOException
	{
	  IndexReader reader = _currentReader;
	  ReaderEntry<R> readerEntry = _diskReaderEntry;
	  
	  if(readerEntry == null || reader != readerEntry.undecorated.getInnerReader())
	  {
	    readerEntry = new ReaderEntry<R>(new ZoieInternalIndexReader(reader), _decorator);
	    _diskReaderEntry = readerEntry;
	  }
	  return readerEntry.decorated;
	}
	
	public Collection<R> getIndexReaders()
	{
	  return _currentReaderDirectory.getDecoratedReaders();
	}
	
	/**
	 * Closes the factory.
	 * 
	 */
    public void close()
    {
      closeReader();
    }
    
    /**
     * Closes the index reader
     */
    public void closeReader()
    {
      _diskReaderEntry = null;
      if(_currentReaderDirectory != null)
      {
        _currentReaderDirectory.dispose();
        _currentReaderDirectory = null;
      }
      if(_currentReader != null)
      {
        try
        {
          _currentReader.close();
        }
        catch(IOException e)
        {
          log.error("problem closing reader", e);
        }
        _currentReader = null;
      }
    }
    
    protected void finalize()
    {
      close();
    }
}
