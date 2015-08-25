package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

public class RAMSearchIndex<R extends IndexReader> extends BaseSearchIndex<R> {
	  private long         _version;
	  private final RAMDirectory _directory;
	  private final LongOpenHashSet       _deletedSet;
	  private final IndexReaderDecorator<R> _decorator;
	  
	  // a consistent pair of reader and deleted set
      private volatile ZoieIndexReader<R> _currentReader;
	  
	  public static final Logger log = Logger.getLogger(RAMSearchIndex.class);

	  RAMSearchIndex(long version, IndexReaderDecorator<R> decorator)
	  {
	    _directory = new RAMDirectory();
	    _version = version;
	    _deletedSet = new LongOpenHashSet();
	    _decorator = decorator;
	    _currentReader = null;
//	    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
//	    cms.setMaxThreadCount(1);
	    _mergeScheduler = new SerialMergeScheduler();
	  }
	  
	  public void close()
	  {
	    super.close();
	    if (_directory!=null)
	    {
	      _directory.close();
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
		ZoieIndexReader<R> reader=null;
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
	  
      public ZoieIndexReader<R> openIndexReader() throws IOException
      {
        return _currentReader;
      }
      

	  @Override
	  protected IndexReader openIndexReaderForDelete() throws IOException {
		if (IndexReader.indexExists(_directory)){
		  return IndexReader.open(_directory,false);
		}
		else{
			return null;
		}
	  }
	  
      private ZoieIndexReader<R> openIndexReaderInternal() throws IOException
      {
	    if (IndexReader.indexExists(_directory))
	    {
	      IndexReader srcReader=null;
	      ZoieIndexReader<R> finalReader=null;
	      try
	      {
	        // for RAM indexes, just get a new index reader
	    	srcReader=IndexReader.open(_directory,true);
	    	finalReader=ZoieIndexReader.open(srcReader, _decorator);
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
	    if(_indexWriter != null) return _indexWriter;
	    
	    // if index does not exist, create empty index
	    boolean create = !IndexReader.indexExists(_directory); 
	    IndexWriter idxWriter = new IndexWriter(_directory, analyzer, create, MaxFieldLength.UNLIMITED); 
	    // TODO disable compound file for RAMDirecory when lucene bug is fixed
	    idxWriter.setUseCompoundFile(true);
	    idxWriter.setMergeScheduler(_mergeScheduler);
	    idxWriter.setRAMBufferSizeMB(3);
	    
	    if (similarity != null)
	    {
	      idxWriter.setSimilarity(similarity);
	    }
	    _indexWriter = idxWriter;
	    return idxWriter;
	  }
	  
	  @Override
	  public void updateIndex(LongSet delDocs, List<IndexingReq> insertDocs,Analyzer analyzer,Similarity similarity)
	    throws IOException
	  {
        super.updateIndex(delDocs, insertDocs, analyzer, similarity);	    

        // we recorded deletes into the delete set only if it is a RAM instance
        _deletedSet.addAll(delDocs);

        ZoieIndexReader<R> reader = null;
        if (_currentReader==null){
        	reader = openIndexReaderInternal();
        }
        else{
        	reader = (ZoieIndexReader<R>)_currentReader.reopen(true);
        }
        if(reader != null) reader.setModifiedSet((LongSet)_deletedSet.clone());
        _currentReader = reader;
	  }
}
