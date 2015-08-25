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
import proj.zoie.api.indexing.OptimizeScheduler;
import proj.zoie.impl.indexing.internal.SearchIndexManager.Status;

public class DiskLuceneIndexDataLoader<R extends IndexReader> extends LuceneIndexDataLoader<R> {

	private long _lastTimeOptimized;
	private static final Logger log = Logger.getLogger(DiskLuceneIndexDataLoader.class);
	private Object _optimizeMonitor;
	private OptimizeScheduler _optScheduler;
	
	public DiskLuceneIndexDataLoader(Analyzer analyzer, Similarity similarity,SearchIndexManager<R> idxMgr) {
		super(analyzer, similarity, idxMgr);
		_lastTimeOptimized=System.currentTimeMillis();
		_optimizeMonitor = new Object();
	}
	
	public void setOptimizeScheduler(OptimizeScheduler scheduler){
		_optScheduler = scheduler;
	}
	
	public OptimizeScheduler getOptimizeScheduler(){
		return _optScheduler;
	}

	@Override
	protected BaseSearchIndex getSearchIndex() {
		return _idxMgr.getDiskIndex();
	}

	@Override
	public void consume(Collection<DataEvent<Indexable>> events)
			throws ZoieException {
		// updates the in memory status before and after the work
		synchronized(_optimizeMonitor)
		{
		  try
		  {
		 /*   _idxMgr.setDiskIndexerStatus(Status.Working);
		    _idxMgr.setPartialExpunge(_optScheduler.getExpunge());
		    try
		    {
		      super.consume(events);
		    }
		    finally
		    {
		      _idxMgr.setPartialExpunge(false);
		      _optScheduler.setExpunge(false);
		    }
            
		    File location = _idxMgr.getDiskIndexLocation();
		    try
		    {
		      FSDirectory idxDir = DiskSearchIndex.getIndexDir(location);
		      int numSegs = IndexUtil.getNumSegments(idxDir);
		      
		      if (numSegs > (_idxMgr.getNumLargeSegments() + 2 * _idxMgr.getMergeFactor()))
		      {
		        try
		        {
		          optimize(_idxMgr.getNumLargeSegments() + 1);
				}
		        catch(IOException e)
		        {
		          throw new ZoieException(e.getMessage(),e);
				}
		      }
		    }
		    catch(IOException ioe)
		    {
		      throw new ZoieException(ioe.getMessage(),ioe);
		    }*/
			  _idxMgr.setDiskIndexerStatus(Status.Working);
			    super.consume(events);
			    File location = _idxMgr.getDiskIndexLocation();
			    try
			    {
			      FSDirectory idxDir = DiskSearchIndex.getIndexDir(location);
			      int numSegs = IndexUtil.getNumSegments(idxDir);
			      
				  int numSegsForOpt = _optScheduler.numSegsScheduledForOptmization(numSegs, _lastTimeOptimized);
			    
			      if (numSegsForOpt > 0){
			        try {
						optimize(numSegsForOpt);
					} catch (IOException e) {
						throw new ZoieException(e.getMessage(),e);
					}
					finally{
						_lastTimeOptimized = System.currentTimeMillis();
					}
			      }
			      _idxMgr.setDiskIndexerStatus(Status.Sleep);
			    }
			    catch(IOException ioe)
			    {
			    	throw new ZoieException(ioe.getMessage(),ioe);
			    }
		  }
		  finally
		  {
            _idxMgr.setDiskIndexerStatus(Status.Sleep);		    
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
            _idxMgr.refreshDiskReader();
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
	        _idxMgr.refreshDiskReader();
		}
		log.info("index optimized");
	}
	
	public long getLastTimeOptimized()
	{
		return _lastTimeOptimized;
	}
}
