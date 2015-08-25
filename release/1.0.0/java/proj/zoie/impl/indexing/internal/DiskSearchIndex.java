package proj.zoie.impl.indexing.internal;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.ZoieIndexReader;

public class DiskSearchIndex extends BaseSearchIndex{
	private final File                 _location;
	  private final IndexReaderDispenser _dispenser;

	  public static final Logger log = Logger.getLogger(DiskSearchIndex.class);

	  DiskSearchIndex(File location)
	  {
	    _location = location;

	    _dispenser = new IndexReaderDispenser(_location);
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

	  /**
	   * Opens an index modifier.
	   * @param analyzer Analyzer
	   * @return IndexModifer instance
	   */
	  public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException
	  {
	    // create the parent directory
	    _location.mkdirs();
	    
	    // get the index signature
	    IndexSignature sig = IndexReaderDispenser.getCurrentIndexSignature(_location);
	    if (sig == null)
	    {
	      File directoryFile = new File(_location, IndexReaderDispenser.INDEX_DIRECTORY);
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
	    File idxDir = new File(_location, sig.getIndexPath());
	    log.debug("opening index writer at: "+idxDir.getAbsolutePath());
	    FSDirectory directory = FSDirectory.getDirectory(idxDir);
	    
	    // create a new modifier to the index, assuming at most one instance is running at any given time
	    boolean create = !IndexReader.indexExists(directory);  
	    IndexWriter idxWriter = new IndexWriter(directory, analyzer, create); 
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
