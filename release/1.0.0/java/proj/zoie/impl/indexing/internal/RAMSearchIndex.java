package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ReadOnlyIndexReader;

public class RAMSearchIndex extends BaseSearchIndex {
	  private long         _version;
	  private RAMDirectory _directory;
	  private IntSet       _deletedSet;
	  
	  public static final Logger log = Logger.getLogger(RAMSearchIndex.class);

	  RAMSearchIndex(long version)
	  {
	    _directory = new RAMDirectory();
	    _version = version;
	    _deletedSet=new IntRBTreeSet();
	  }
	  
	  public void close()
	  {
	    if (_directory!=null)
	    {
	      _directory.close();
	    }
	  }
	  
	  /**
	   * Gets the internal deleted set, only applies to RAM indexes
	   * @return
	   */
	  public IntSet getDeletedSet()
	  {
	    return _deletedSet;
	  }

	  public long getVersion()
	  {
	    return _version;
	  }

	  public void setVersion(long version)
	      throws IOException
	  {
	    _version = version;
	  }

	  public int getNumdocs()
	  {
		ZoieIndexReader reader=null;
	    try
	    {
	      reader=openIndexReader();
	    }
	    catch (IOException e)
	    {
	      log.error(e.getMessage(),e);
	    }
	    
	    if (reader!=null)
	    {
	      return reader.numDocs();
	    }
	    else
	    {
	      return 0;
	    }
	  }
	  
	  public ZoieIndexReader openIndexReader() throws IOException
	  {
	    if (IndexReader.indexExists(_directory))
	    {
	      IndexReader srcReader=null;
	      ZoieIndexReader finalReader=null;
	      try
	      {
	        // for RAM indexes, just get a new index reader
	    	srcReader=IndexReader.open(_directory);
	    	finalReader=new ZoieIndexReader(new ReadOnlyIndexReader(srcReader));
	        return finalReader;
	      }
	      catch(IOException ioe)
	      {
	        // if reader decoration fails, still need to close the source reader
	        if (srcReader!=null)
	        {
	        	srcReader.close();
	        }
	        throw ioe;
	      }
	    }
	    else{
	      return null;            // null indicates no index exist, following the contract
	    }
	  }

	  public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity)
	    throws IOException
	  {
	    // if index does not exist, create empty index
	    boolean create = !IndexReader.indexExists(_directory); 
	    IndexWriter idxWriter = new IndexWriter(_directory, analyzer, create); 
	    if (similarity != null)
	    {
	      idxWriter.setSimilarity(similarity);
	    }
	    return idxWriter;
	  }
}
