package proj.zoie.impl.indexing.internal;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class IndexReaderDispenser {
	private static final Logger log = Logger.getLogger(IndexReaderDispenser.class);
	
	private static final int INDEX_OPEN_NUM_RETRIES=5;
	
	public static final String  INDEX_DIRECTORY = "index.directory";
	public static final String   INDEX_DIR_NAME = "beef";

	private static final class InternalIndexReader extends ZoieIndexReader {
		private IndexSignature _sig;
		InternalIndexReader(IndexReader in,IndexReaderDecorator<?> decorator, IndexSignature sig) throws IOException
		{
			super(in, decorator);
			_sig=sig;
		}
		
		@Override
		public void decRef() throws IOException {			
		}

		@Override
		public void incRef() {
		}
	}
	
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

    private volatile InternalIndexReader _currentReader;
	private final IndexReaderDecorator<?> _decorator;
	private final File _indexHome;
	
	public IndexReaderDispenser(File indexHome, IndexReaderDecorator<?> decorator)
	{
	  _indexHome = indexHome;
	  _decorator = decorator;
	  
	  IndexSignature sig = getCurrentIndexSignature(_indexHome);
	  if(sig != null)
	  {
	    try
	    {
	      getNewReader();
	    }
	    catch (IOException e)
	    {
	      log.error(e);
	    }
	  }
	}
	
	public long getCurrentVersion()
	{
		return _currentReader!=null ? (_currentReader._sig!=null ? _currentReader._sig.getVersion(): 0L): 0L;
	}
	
	/**
	   * constructs a new IndexReader instance
	   * 
	   * @param indexPath
	   *            Where the index is.
	   * @return Constructed IndexReader instance.
	   * @throws IOException
	   */
	  private InternalIndexReader newReader(File luceneDir, IndexReaderDecorator<?> decorator, IndexSignature signature)
	      throws IOException
	  {
	    if (!luceneDir.exists() || !IndexReader.indexExists(luceneDir))
	      return null;
	    Directory dir=FSDirectory.getDirectory(luceneDir);
	    int numTries=INDEX_OPEN_NUM_RETRIES;
	    InternalIndexReader reader=null;
	    
	    // try max of 5 times, there might be a case where the segment file is being updated
	    while(reader==null)
	    {
	      if (numTries==0)
	      {
	        log.error("Problem refreshing disk index, all attempts failed.");
	        throw new IOException("problem opening new index");
	      }
	      numTries--;
	      
	      try{
	        if(log.isDebugEnabled())
	        {
	          log.debug("opening index reader at: "+luceneDir.getAbsolutePath());
	        }
	        IndexReader srcReader = IndexReader.open(dir,true);
	        
	        try
	        {
	          reader=new InternalIndexReader(srcReader, decorator, signature);
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
	    return reader;
	  }

	  /**
	   * get a fresh new reader instance
	   * @return an IndexReader instance, can be null if index does not yet exit
	   * @throws IOException
	   */
	  public ZoieIndexReader getNewReader() throws IOException
	  {
	      int numTries=INDEX_OPEN_NUM_RETRIES;   
	      InternalIndexReader reader=null;
	      
	      // try it for a few times, there is a case where lucene is swapping the segment file, 
	      // or a case where the index directory file is updated, both are legitimate,
	      // trying again does not block searchers,
	      // the extra time it takes to get the reader, and to sync the index, memory index is collecting docs
	     
	      while(reader==null)
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
	          String luceneDir = sig.getIndexPath();
	    
	          if (luceneDir == null || luceneDir.trim().length() == 0)
	          {
	            throw new IOException(INDEX_DIRECTORY + " contains no data.");
	          }
	          
	          if (luceneDir != null)
	          {
	        	reader = newReader(new File(_indexHome,luceneDir), _decorator, sig);
	            break;
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
	      
	      // do decoration before swapping
	      if(reader != null) reader.getDecoratedReader();
	      
	      // swap the internal readers
	      _currentReader = reader;
	      
	      return reader;
	    }
	
	public ZoieIndexReader getIndexReader()
	{
		return _currentReader;
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
