package proj.zoie.impl.indexing.internal;


import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexUtil;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.Indexable;
import proj.zoie.impl.indexing.internal.SearchIndexManager.Status;

public class DiskLuceneIndexDataLoader<R extends IndexReader> extends LuceneIndexDataLoader<R> {

	private long _lastTimeOptimized;
	private long _optimizationDuration;
	private int _maxSegments;
	
	private static long DAY_IN_MILLIS = 1000L*60L*60L*24L;
    private static int DEFAULT_MAX_SEGMENTS = 20;
	private static final Logger log = Logger.getLogger(DiskLuceneIndexDataLoader.class);
	private Object _optimizeMonitor;
	
	public DiskLuceneIndexDataLoader(Analyzer analyzer, Similarity similarity,SearchIndexManager<R> idxMgr) {
		super(analyzer, similarity, idxMgr);
		_lastTimeOptimized=System.currentTimeMillis();
		_optimizationDuration=DAY_IN_MILLIS;
		_optimizeMonitor = new Object();
		_maxSegments = DEFAULT_MAX_SEGMENTS;
	}

	@Override
	protected BaseSearchIndex getSearchIndex() {
		return _idxMgr.getDiskIndex();
	}
	
	public void setMaxSegments(int maxSegments)
	{
	  _maxSegments = maxSegments;
	}
	
	public int getMaxSegments()
	{
	  return _maxSegments;
	}

	@Override
	public void consume(Collection<DataEvent<Indexable>> events)
			throws ZoieException {
		// updates the in memory status before and after the work
		synchronized(_optimizeMonitor)
		{
			_idxMgr.setDiskIndexerStatus(Status.Working);
		    super.consume(events);
		    File location = _idxMgr.getDiskIndexLocation();
		    try
		    {
		      FSDirectory idxDir = DiskSearchIndex.getIndexDir(location);
		      int numSegs = IndexUtil.getNumSegments(idxDir);
		    
		      long now=System.currentTimeMillis();
		      if ((now-_lastTimeOptimized) > _optimizationDuration || numSegs >= _maxSegments)
		      {
		        try {
					optimize(_maxSegments);
				} catch (IOException e) {
					throw new ZoieException(e.getMessage(),e);
				}
		      }
		      _idxMgr.setDiskIndexerStatus(Status.Sleep);
		    }
		    catch(IOException ioe)
		    {
		    	throw new ZoieException(ioe.getMessage(),ioe);
		    }
		}
	}
	
	public void expungeDeletes() throws IOException
	{
		log.info("expunging deletes...");
		synchronized(_optimizeMonitor)
		{
			BaseSearchIndex idx=getSearchIndex();
	        IndexWriter writer=null;  
	        try
	        {
	          writer=idx.openIndexWriter(_analyzer, _similarity);
	          writer.expungeDeletes(true);
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
		log.info("deletes expunged");
	}
	
	public void optimize(int numSegs) throws IOException
	{
		if (numSegs<=1) numSegs = 1;
		log.info("optmizing, numSegs: "+numSegs+" ...");
		// we should optimize
		synchronized(_optimizeMonitor)
		{
	    	BaseSearchIndex idx=getSearchIndex();
	        IndexWriter writer=null;  
	        try
	        {
	          writer=idx.openIndexWriter(_analyzer, _similarity);
	          writer.optimize(numSegs);
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
		log.info("index optimized");
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
