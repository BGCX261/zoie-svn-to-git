package proj.zoie.impl.indexing.internal;


import java.io.IOException;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.Indexable;
import proj.zoie.impl.indexing.internal.SearchIndexManager.Status;

public class DiskLuceneIndexDataLoader<R extends IndexReader> extends LuceneIndexDataLoader<R> {

	private long _lastTimeOptimized;
	private long _optimizationDuration;
	
	private static long DAY_IN_MILLIS = 1000L*60L*60L*24L;
	private static final Logger log = Logger.getLogger(DiskLuceneIndexDataLoader.class);
	public DiskLuceneIndexDataLoader(Analyzer analyzer, Similarity similarity,SearchIndexManager<R> idxMgr) {
		super(analyzer, similarity, idxMgr);
		_lastTimeOptimized=System.currentTimeMillis();
		_optimizationDuration=DAY_IN_MILLIS;
	}

	@Override
	protected BaseSearchIndex getSearchIndex() {
		return _idxMgr.getDiskIndex();
	}

	@Override
	public void consume(Collection<DataEvent<Indexable>> events)
			throws ZoieException {
		// updates the in memory status before and after the work
		_idxMgr.setDiskIndexerStatus(Status.Working);
	    super.consume(events);
	      
	      long now=System.currentTimeMillis();
	     if ((now-_lastTimeOptimized) > _optimizationDuration)
	      {
	        try {
				optimize();
			} catch (IOException e) {
				throw new ZoieException(e.getMessage(),e);
			}
	      }
	      _idxMgr.setDiskIndexerStatus(Status.Sleep);
	}
	
	public void optimize() throws IOException
	{
		// we should optimize
    	BaseSearchIndex idx=getSearchIndex();
        IndexWriter writer=null;  
        try
        {
          writer=idx.openIndexWriter(_analyzer, _similarity);
          writer.optimize(true);
        }
        finally
        {
        	if (writer!=null)
        	{
        		try {
					writer.close();
				} catch (CorruptIndexException e) {
					log.fatal("possible index corruption! "+e.getMessage());
				} catch (IOException e) {
					log.error(e.getMessage(),e);
				}
        	}
        }
	}
	
	public void setOptimizationDuration(long optimizationDuration)
	{
		_optimizationDuration=optimizationDuration;
	}
	
	public long getOptimizationDuration()
	{
		return _optimizationDuration;
	}
	
	public long getLastTimeOptimized()
	{
		return _lastTimeOptimized;
	}
}
