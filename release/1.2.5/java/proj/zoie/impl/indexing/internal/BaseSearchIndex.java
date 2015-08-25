package proj.zoie.impl.indexing.internal;

import java.io.IOException;
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.Indexable;

public abstract class BaseSearchIndex {
	  private int _eventsHandled=0;
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
	  abstract public ZoieIndexReader openIndexReader() throws IOException;
	  
	  public void updateIndex(IntSet delDocs, List<Document> insertDocs,Analyzer analyzer,Similarity similarity)
	      throws IOException
	  {
	    IndexWriter idxMod = null;
	    try
	    {
	      idxMod = openIndexWriter(analyzer,similarity);
	      if (idxMod != null)
	      {
	        for (int docid : delDocs)
	        {
	          Term t = new Term(Indexable.DOCUMENT_ID_FIELD,String.valueOf(docid));
	          idxMod.deleteDocuments(t);
	        }
	        
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
