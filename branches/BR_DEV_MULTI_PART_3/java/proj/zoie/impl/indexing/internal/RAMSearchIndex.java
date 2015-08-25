package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.ZoieInternalIndexReader;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;
import proj.zoie.impl.indexing.internal.ReaderDirectory.ReaderEntry;

public class RAMSearchIndex<R extends IndexReader> extends BaseSearchIndex<R> {
	  private long         _version;
	  private final RAMDirectory _directory;
	  private final IntOpenHashSet       _deletedSet;
	  private final IndexReaderDecorator<R> _decorator;
	  
	  // a consistent pair of reader and deleted set
	  private volatile ReaderEntry<R> _currentReaderEntry;
	  
	  public static final Logger log = Logger.getLogger(RAMSearchIndex.class);

	  RAMSearchIndex(long version, IndexReaderDecorator<R> decorator)
	  {
	    _directory = new RAMDirectory();
	    _version = version;
	    _deletedSet = new IntOpenHashSet();
	    _decorator = decorator;
	    _currentReaderEntry = null;
	    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
	    cms.setMaxThreadCount(1);
	    _mergeScheduler = cms;
	  }
	  
	  public void close()
	  {
	    if (_directory!=null)
	    {
	      _directory.close();
	    }
	  }
	  
	  public void refresh() throws IOException
	  {
	    ZoieIndexReader reader = openIndexReaderInternal();
	    if(reader != null)
	    {
	      reader.setModifiedSet((IntSet)_deletedSet.clone());
	      ReaderEntry<R> tmp = new ReaderEntry<R>(reader,_decorator);
          _currentReaderEntry = tmp;
	    }
	    else
	    {
	      _currentReaderEntry = null;
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
	      reader=openIndexReaderInternal();
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
	  
	  @Override
      public Collection<R> openIndexReaders() throws IOException{
    	List<R> readerList = new ArrayList<R>(1);
    	if (_currentReaderEntry!=null){
    	  readerList.add(_currentReaderEntry.decorated);
    	}
    	return readerList;
      }
      
	  public ReaderEntry<R> getCurrentReaderEntry(){
		  return _currentReaderEntry;
	  }
	  
	  private IndexReader openIndexReaderForDelete() throws IOException {
		if (IndexReader.indexExists(_directory)){
		  return IndexReader.open(_directory,false);
		}
		else{
			return null;
		}
	  }
	  
	  @Override
	  protected void deleteDocs(IntSet delDocs) throws IOException
	  {
	    int[] delArray=BaseSearchIndex.findDelDocIds(_currentReaderEntry == null ? null : _currentReaderEntry.undecorated,delDocs);
	    if (delArray!=null && delArray.length > 0)
	    {
	      IndexReader readerForDelete = null;
	      try
	      {
	        readerForDelete = openIndexReaderForDelete();
	        if (readerForDelete!=null)
	        {
	          for (int docid : delArray)
	          {
	            readerForDelete.deleteDocument(docid);
	          }
	        }
	      }
	      finally
	      {
	        if (readerForDelete!=null)
	        {
	          try
	          {
	            readerForDelete.close();
	          }
	          catch(IOException ioe)
	          {
	            log.error(ioe.getMessage(),ioe);
	          }
	        }
	      }
	    }
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
	    	finalReader=new ZoieInternalIndexReader(srcReader);
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
	    // TODO disable compound file for RAMDirecory when lucene bug is fixed
	    idxWriter.setUseCompoundFile(true);
	    idxWriter.setMergeScheduler(_mergeScheduler);
	    
	    if (similarity != null)
	    {
	      idxWriter.setSimilarity(similarity);
	    }
	    return idxWriter;
	  }
	  
	  @Override
	  public void updateIndex(IntSet delDocs, List<IndexingReq> insertDocs,Analyzer analyzer,Similarity similarity)
	    throws IOException
	  {
        super.updateIndex(delDocs, insertDocs, analyzer, similarity);	    

        // we recorded deletes into the delete set only if it is a RAM instance
        _deletedSet.addAll(delDocs);

        refresh();
	  }
}
