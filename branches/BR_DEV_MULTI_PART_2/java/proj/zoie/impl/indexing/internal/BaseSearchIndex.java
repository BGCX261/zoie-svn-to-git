package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.util.IntSetAccelerator;
import proj.zoie.impl.indexing.internal.ReaderDirectory.ReaderEntry;

public abstract class BaseSearchIndex<R extends IndexReader> {
	  private static final Logger log = Logger.getLogger(BaseSearchIndex.class);
	  private int _eventsHandled=0;
	  protected MergeScheduler _mergeScheduler;
	  
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
	  abstract public void close();
	  abstract public Collection<R> openIndexReaders() throws IOException;
	  
	  public static int[] findDelDocIds(ZoieIndexReader reader, IntSet delDocs){
		  int[] delArray=null;
		  if (delDocs!=null && delDocs.size() > 0){
		    if (reader!=null){
		        int[] uidArray = reader.getUIDArray();
		        
		        IntList delList = new IntArrayList(delDocs.size());
		        for (int i=0;i<uidArray.length;++i)
		        {
		          int uid = uidArray[i];
		          if (uid >=0 && delDocs.contains(uid))
		          {
		            delList.add(i);
		          }
		        }
		        delArray = delList.toIntArray();
		    }
		  }
		  return delArray;
	  }
	  
	  public void updateIndex(IntSet delDocs, List<Document> insertDocs,Analyzer analyzer,Similarity similarity)
	      throws IOException
	  {
	    deleteDocs(delDocs);
		
	    IndexWriter idxMod = null;
	    try
	    {
	      idxMod = openIndexWriter(analyzer,similarity);
	      if (idxMod != null)
	      { 
	        for (Document doc : insertDocs)
	        {
	          idxMod.addDocument(doc);
	        }
	      }
	    }
	    finally
	    {
	      if (idxMod!=null)
	      {
	        idxMod.close();
	      }
	    }
	  }
	  
	  abstract protected void deleteDocs(IntSet delDocs) throws IOException;
	  
	  public void loadFromIndex(RAMSearchIndex<R> index) throws IOException
	  {
		ReaderEntry<R> readerEntry = index.getCunrrentReaderEntry();
		if (readerEntry == null) return;
	    ZoieIndexReader reader = readerEntry.undecorated;
	    IntSet delSet = new IntSetAccelerator(reader.getModifiedSet());
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
	      if(writer != null) writer.close();
	    }
	  }
	      
	  abstract public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException;

	  public void incrementEventCount(int eventCount)
	  {
	    _eventsHandled+=eventCount;
	  }
	  
	  public int getEventsHandled()
	  {
	    return _eventsHandled;
	  }
}
