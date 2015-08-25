package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.ZoieInternalIndexReader;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import proj.zoie.api.indexing.IndexReaderDecorator;

public class DiskSearchIndex<R extends IndexReader> extends BaseSearchIndex<R>{
	  private final File                 _location;
	  private final IndexReaderDispenser<R> _dispenser;
	  
	  private MergePolicy _mergePolicy;

	  public static final Logger log = Logger.getLogger(DiskSearchIndex.class);

	  DiskSearchIndex(File location, IndexReaderDecorator<R> decorator, MergePolicy mergePolicy)
	  {
	    _location = location;

	    _dispenser = new IndexReaderDispenser<R>(_location, decorator);
	    
	    _mergePolicy = mergePolicy;
	    _mergeScheduler = new SerialMergeScheduler();
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
	    return _dispenser.numDocs();
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
	  
	  /**
	   * Refreshes the index reader. Actual creation of a new index reader is deferred
	   */
	  public void refresh() throws IOException
	  {
	    _dispenser.closeReader();
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
        idxWriter.setMergeScheduler(_mergeScheduler);
        idxWriter.setMergePolicy(_mergePolicy);
	    
	    if (similarity != null)
	    {
	    	idxWriter.setSimilarity(similarity);
	    }
	    return idxWriter;
	  }
	  
	  /**
	   * Gets the current reader
	   */
	  @Override
	  public Collection<R> openIndexReaders() throws IOException
	  {
	    // use dispenser to get the reader
	    return _dispenser.getIndexReaders();
	  }
	  
	  public R getDiskIndexReader() throws IOException
	  {
	    return _dispenser.getDiskIndexReader();
	  }

	/**
	   * Gets a new reader directory, force a reader refresh
	   * @return
	   * @throws IOException
	   */
	  public ReaderDirectory<R> getNewReaderDirectory() throws IOException
	  {
		return _dispenser.getNewReaderDirectory();
	  }
	  
	  @Override
	  protected void deleteDocs(IntSet delDocs) throws IOException{
		  File luceneDir = _dispenser.getLuceneDir();
		  if (luceneDir != null && luceneDir.exists()){
		    ZoieInternalIndexReader.deleteDocs(FSDirectory.getDirectory(luceneDir), _dispenser.getCurrentReaderDirectory(),delDocs);
		  }
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
