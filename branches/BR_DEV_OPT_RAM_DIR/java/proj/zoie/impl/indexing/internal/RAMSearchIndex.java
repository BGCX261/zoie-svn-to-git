package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import proj.zoie.api.ZoieIndexReader;

public class RAMSearchIndex extends BaseSearchIndex {
	
	  
	  private long         _version;
	  private final Directory _directory;
	  private final IntSet       _deletedSet;
	  
	  // a consistent pair of reader and deleted set
      private volatile ZoieIndexReader _currentReader;
	  
	  public static final Logger log = Logger.getLogger(RAMSearchIndex.class);

	  RAMSearchIndex(long version)
	  {
	    _directory = RTDirectoryFactory.DEFAULT.newRamDirectory();
	    _version = version;
	    _deletedSet = new IntRBTreeSet();
	    _currentReader = null;
	  }
	  
	  public void close()
	  {
	    if (_directory!=null)
	    {
	      try {
			_directory.close();
		  } catch (IOException e) {
			log.error(e.getMessage(),e);
		  }
	    }
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
        return _currentReader;
      }
	  
      private ZoieIndexReader openIndexReaderInternal() throws IOException
      {
	    if (IndexReader.indexExists(_directory))
	    {
	      IndexReader srcReader=null;
	      ZoieIndexReader finalReader=null;
	      try
	      {
	        // for RAM indexes, just get a new index reader
	    	srcReader=IndexReader.open(_directory,true);
	    	finalReader=new ZoieIndexReader(srcReader);
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
	    IndexWriter idxWriter = new IndexWriter(_directory, analyzer, create, MaxFieldLength.UNLIMITED); 
	    if (similarity != null)
	    {
	      idxWriter.setSimilarity(similarity);
	    }
	    return idxWriter;
	  }
	  
	  public void updateIndex(IntSet delDocs, List<Document> insertDocs,Analyzer analyzer,Similarity similarity)
	    throws IOException
	  {
        super.updateIndex(delDocs, insertDocs, analyzer, similarity);	    

        // we recorded deletes into the delete set only if it is a RAM instance
        _deletedSet.addAll(delDocs);

        ZoieIndexReader reader = openIndexReaderInternal();
        if(reader != null) reader.setModifiedSet(new IntRBTreeSet(_deletedSet));
        _currentReader = reader;
	  }
}
