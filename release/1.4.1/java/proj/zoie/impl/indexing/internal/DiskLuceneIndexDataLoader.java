package proj.zoie.impl.indexing.internal;


import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.OptimizeScheduler;
import proj.zoie.api.indexing.OptimizeScheduler.OptimizeType;
import proj.zoie.api.indexing.ZoieIndexable;
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
	public void consume(Collection<DataEvent<ZoieIndexable>> events)
			throws ZoieException {
		// updates the in memory status before and after the work
		synchronized(_optimizeMonitor)
		{
		  try
		  {
		    _idxMgr.setDiskIndexerStatus(Status.Working);
		    OptimizeType optType = _optScheduler.getScheduledOptimizeType();
		    _idxMgr.setPartialExpunge(optType == OptimizeType.PARTIAL);
		    try
		    {
		      super.consume(events);
		    }
		    finally
		    {
		      _optScheduler.finished();
		      _idxMgr.setPartialExpunge(false);
		    }
		    
		    if(optType == OptimizeType.FULL)
		    {
	          try
	          {
	            expungeDeletes();
	          }
	          catch(IOException ioe)
	          {
	            throw new ZoieException(ioe.getMessage(),ioe);
	          }
	          finally
	          {
	            _optScheduler.finished();
	          }
		    }
		  }
		  finally
		  {
            _idxMgr.setDiskIndexerStatus(Status.Sleep);		    
		  }
		}
	}
	
	@Override
    public void loadFromIndex(RAMSearchIndex ramIndex) throws ZoieException
    {
      synchronized(_optimizeMonitor)
      {
        try
        {
          _idxMgr.setDiskIndexerStatus(Status.Working);
          
          OptimizeType optType = _optScheduler.getScheduledOptimizeType();
          _idxMgr.setPartialExpunge(optType == OptimizeType.PARTIAL);
          try
          {
            super.loadFromIndex(ramIndex);
          }
          finally
          {
            _optScheduler.finished();
            _idxMgr.setPartialExpunge(false);
          }
          
          if(optType == OptimizeType.FULL)
          {
            try
            {
              expungeDeletes();
            }
            catch(IOException ioe)
            {
              throw new ZoieException(ioe.getMessage(),ioe);
            }
            finally
            {
              _optScheduler.finished();
            }
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
	
	public long exportSnapshot(WritableByteChannel channel) throws IOException
	{
	  DiskSearchIndex idx = (DiskSearchIndex)getSearchIndex();
	  if(idx != null)
	  {
	    DiskIndexSnapshot snapshot = null;
        
	    try
	    {
	      synchronized(_optimizeMonitor) // prevent index updates while taking a snapshot
	      {
	        snapshot = idx.getSnapshot();
	      }
	      
	      return (snapshot != null ?  snapshot.writeTo(channel) : 0);
	    }
	    finally
	    {
	      if(snapshot != null) snapshot.close();
	    }
	  }
	  return 0;
	}
	
	public void importSnapshot(ReadableByteChannel channel) throws IOException
	{
      DiskSearchIndex idx = (DiskSearchIndex)getSearchIndex();
      if(idx != null)
      {
        synchronized(_optimizeMonitor) // prevent index updates while taking a snapshot
        {
	      _idxMgr.purgeIndex();
	      idx.importSnapshot(channel);
	      _idxMgr.refreshDiskReader();
	    }
	  }
	}
}
