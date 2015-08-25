package proj.zoie.impl.indexing.internal;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class DiskSearchIndex extends BaseSearchIndex{
	  private final File                 _location;
	  private final IndexReaderDispenser _dispenser;
	  
	  private int _mergeFactor;
	  private int _maxMergeDocs;
	  private boolean _useCompoundFile;

	  public static final Logger log = Logger.getLogger(DiskSearchIndex.class);

	  DiskSearchIndex(File location, IndexReaderDecorator<?> decorator)
	  {
	    _location = location;

	    _dispenser = new IndexReaderDispenser(_location, decorator);
	    
	    _useCompoundFile = true;
	    
	    _mergeFactor = LogMergePolicy.DEFAULT_MERGE_FACTOR;
		_maxMergeDocs = LogMergePolicy.DEFAULT_MAX_MERGE_DOCS;
	  }
	  
	  public boolean isUseCompoundFile()
	  {
		return _useCompoundFile;
	  }
	  
	  public void setUseCompoundFile(boolean useCompoundFile)
	  {
		_useCompoundFile = useCompoundFile;  
	  }
	  
	  public void setMergeFactor(int mergeFactor)
	  {
		if (mergeFactor >=0 )
		{
		  _mergeFactor = mergeFactor;
		}
	  }
		
	  public int getMergeFactor()
	  {
		return _mergeFactor;
	  }
	
	  public void setMaxMergeDocs(int maxMergeDocs)
	  {
		if (maxMergeDocs >= 0)
		{
			_maxMergeDocs = maxMergeDocs;
		}
	  }
	
 	  public int getMaxMergeDocs()
	  {
		return _maxMergeDocs;
	  }

	  public long getVersion()
	  {
	    return _dispenser.getCurrentVersion();
	  }
	  
	  /**
	   * Gets the number of docs in the current loaded index
	   * @return number of docs
	   */
	  public int getNumdocs()
	  {
	    IndexReader reader=_dispenser.getIndexReader();
	    if (reader!=null)
	    {
	      return reader.numDocs();
	    }
	    else
	    {
	      return 0;
	    }
	  }

	  /**
	   * Close and releases dispenser and clean up
	   */
	  public void close()
	  {
	    // close the dispenser
	    if (_dispenser != null)
	    {
	        _dispenser.close();
	    }
	  }

	  @Override
	  protected void finalize()
	  {
	    close();
	  }
	  
	  public static FSDirectory getIndexDir(File location) throws IOException
	  {
		IndexSignature sig = null;
		if (location.exists())
		{
		  sig = IndexReaderDispenser.getCurrentIndexSignature(location);
		}
		  
		if (sig == null)
	    {
	      File directoryFile = new File(location, IndexReaderDispenser.INDEX_DIRECTORY);
	      sig = new IndexSignature(IndexReaderDispenser.INDEX_DIR_NAME, 0L);
	      try
	      {
	        sig.save(directoryFile);
	      }
	      catch (IOException e)
	      {
	        throw e;
	      }
	    }
		

	    File idxDir = new File(location, sig.getIndexPath());
	    FSDirectory directory = NIOFSDirectory.getDirectory(idxDir);
	    
	    return directory;
	  }

	  /**
	   * Opens an index modifier.
	   * @param analyzer Analyzer
	   * @return IndexModifer instance
	   */
	  public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException
	  {
	    // create the parent directory
	    _location.mkdirs();
	    
	    FSDirectory directory = getIndexDir(_location);

	    log.info("opening index writer at: "+directory.getFile().getAbsolutePath());
	    
	    // create a new modifier to the index, assuming at most one instance is running at any given time
	    boolean create = !IndexReader.indexExists(directory);  
	    IndexWriter idxWriter = new IndexWriter(directory, analyzer, create,MaxFieldLength.UNLIMITED); 
	    idxWriter.setUseCompoundFile(_useCompoundFile);
	    idxWriter.setMaxMergeDocs(_maxMergeDocs);
	    idxWriter.setMergeFactor(_mergeFactor);
	    if (similarity != null)
	    {
	    	idxWriter.setSimilarity(similarity);
	    }
	    return idxWriter;
	  }
	  
	  /**
	   * Gets the current reader
	   */
	  public ZoieIndexReader openIndexReader() throws IOException
	  {
	    // use dispenser to get the reader
	    return _dispenser.getIndexReader();
	  }
	  
	  /**
	   * Gets a new reader, force a reader refresh
	   * @return
	   * @throws IOException
	   */
	  public ZoieIndexReader getNewReader() throws IOException
	  {
	    return _dispenser.getNewReader();
	  }
	  
	  /**
	   * Writes the current version/SCN to the disk
	   */
	  public void setVersion(long version)
	      throws IOException
	  {
	    // update new index file
	    File directoryFile = new File(_location, IndexReaderDispenser.INDEX_DIRECTORY);
	    IndexSignature sig = IndexSignature.read(directoryFile);
	    sig.updateVersion(version);
	    try
	    {
	      // make sure atomicity of the index publication
	      File tmpFile = new File(_location, IndexReaderDispenser.INDEX_DIRECTORY + ".new");
	      sig.save(tmpFile);
	      File tmpFile2 = new File(_location, IndexReaderDispenser.INDEX_DIRECTORY + ".tmp");
	      directoryFile.renameTo(tmpFile2);
	      tmpFile.renameTo(directoryFile);
	      tmpFile2.delete();
	    }
	    catch (IOException e)
	    {
	      throw e;
	    }

	  }
}
