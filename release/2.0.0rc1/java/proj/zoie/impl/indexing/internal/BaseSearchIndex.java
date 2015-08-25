package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

public abstract class BaseSearchIndex<R extends IndexReader> {
	  private static final Logger log = Logger.getLogger(BaseSearchIndex.class);
	  
	  private int _eventsHandled=0;
	  protected MergeScheduler _mergeScheduler;
	  protected IndexWriter _indexWriter = null;
	  
	  /**
	   * gets index version, e.g. SCN
	   * @return index version
	   */
	  abstract public long getVersion();
	  
	  /**
	   * gets number of docs in the index, .e.g maxdoc - number of deleted docs
	   * @return
	   */
	  abstract public int getNumdocs();

	  /**
	   * Sets the index version
	   * @param version
	   * @throws IOException
	   */
	  abstract public void setVersion(long version)
	      throws IOException;
	  
	  /**
	   * close and free all resources
	   */
	  public void close()
	  {
	    closeIndexWriter();
	  }
	  
	  abstract public ZoieIndexReader<R> openIndexReader() throws IOException;
	  
	  abstract protected IndexReader openIndexReaderForDelete() throws IOException;
	  
	  public void updateIndex(LongSet delDocs, List<IndexingReq> insertDocs,Analyzer defaultAnalyzer,Similarity similarity)
	      throws IOException
	  {
	    deleteDocs(delDocs);
		
	    IndexWriter idxMod = null;
	    try
	    {
	      idxMod = openIndexWriter(defaultAnalyzer,similarity);
	      if (idxMod != null)
	      { 
	        for (IndexingReq idxPair : insertDocs)
	        {
	          Analyzer analyzer = idxPair.getAnalyzer();
	          Document doc = idxPair.getDocument();
	          if (analyzer == null){
	            idxMod.addDocument(doc);
	          }
	          else{
	        	idxMod.addDocument(doc,analyzer);
	          }
	        }
	      }
	    }
	    finally
	    {
	      if (idxMod!=null)
	      {
	        idxMod.commit();
	      }
	    }
	  }
	  
	  private void deleteDocs(LongSet delDocs) throws IOException
	  {
		int[] delArray=null;
	    if (delDocs!=null && delDocs.size() > 0)
	    {
	      ZoieIndexReader<R> reader= openIndexReader();
	      if (reader!=null)
	      {

		    IntList delList = new IntArrayList(delDocs.size());
	    	DocIDMapper idMapper = reader.getDocIDMaper();
	    	LongIterator iter = delDocs.iterator();
	        
	    	while(iter.hasNext()){
	    		long uid = iter.nextLong();
	    		if (ZoieIndexReader.DELETED_UID!=uid){
		    		int docid = idMapper.getDocID(uid);
		    		if (docid!=DocIDMapper.NOT_FOUND){
		    			delList.add(docid);
		    		}
	    		}
	    	}
	        delArray = delList.toIntArray();
	      }
	    }
	      
	    if (delArray!=null && delArray.length > 0)
	    {
	      closeIndexWriter();
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
	  
	  public void loadFromIndex(BaseSearchIndex<R> index) throws IOException
	  {
	    ZoieIndexReader<R> reader = index.openIndexReader();
	    if(reader == null) return;
	    
	    LongSet delSet = reader.getModifiedSet();
	    Directory dir = reader.directory();
	    
	    deleteDocs(delSet);
	    
	    IndexWriter writer = null;
	    try
	    {
	      writer = openIndexWriter(null,null);
	      writer.addIndexesNoOptimize(new Directory[] { dir });
	    }
	    finally
	    {
	      closeIndexWriter();
	    }
	  }
	      
	  abstract public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException;
	  
	  public void closeIndexWriter()
      {
        if(_indexWriter != null)
        {
          try
          {
            _indexWriter.close();
          }
          catch(Exception e)
          {
            log.error(e.getMessage(), e);
          }
          _indexWriter = null;
        }
      }
	  
	  public void incrementEventCount(int eventCount)
	  {
	    _eventsHandled+=eventCount;
	  }
	  
	  public int getEventsHandled()
	  {
	    return _eventsHandled;
	  }
}
