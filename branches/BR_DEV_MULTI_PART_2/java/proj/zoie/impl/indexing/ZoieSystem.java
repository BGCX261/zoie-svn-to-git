package proj.zoie.impl.indexing;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.api.indexing.DefaultOptimizeScheduler;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.Indexable;
import proj.zoie.api.indexing.IndexableInterpreter;
import proj.zoie.api.indexing.OptimizeScheduler;
import proj.zoie.impl.indexing.internal.BatchedIndexDataLoader;
import proj.zoie.impl.indexing.internal.CopyingIndexDataLoader;
import proj.zoie.impl.indexing.internal.DelegateIndexDataConsumer;
import proj.zoie.impl.indexing.internal.DiskLuceneIndexDataLoader;
import proj.zoie.impl.indexing.internal.IndexReaderDispenser;
import proj.zoie.impl.indexing.internal.RAMLuceneIndexDataLoader;
import proj.zoie.impl.indexing.internal.SearchIndexManager;
import proj.zoie.mbean.ZoieSystemAdminMBean;

public class ZoieSystem<R extends IndexReader,V> extends AsyncDataConsumer<V> implements DataConsumer<V>,IndexReaderFactory<R> {

	private static final Logger log = Logger.getLogger(ZoieSystem.class);
	
	private final File _idxDir;
	private final boolean _realtimeIndexing;
	private final SearchIndexManager<R> _searchIdxMgr;
	private IndexableInterpreter<V> _interpreter;
	private Analyzer _analyzer;
	private Similarity _similarity;
	private RAMLuceneIndexDataLoader<R> _ramLoader;
	private BatchedIndexDataLoader<R,Indexable>  _batchedDiskLoader;
	private DiskLuceneIndexDataLoader<R> _diskLoader;
	private DelegateIndexDataConsumer<V> _delegateConsumer;
	private List<IndexingEventListener> _lsnrList;
	
	public ZoieSystem(File idxDir,IndexableInterpreter<V> interpreter,IndexReaderDecorator<R> indexReaderDecorator,Analyzer analyzer,Similarity similarity,int batchSize,long batchDelay,boolean rtIndexing)
	{
		if (idxDir==null) throw new IllegalArgumentException("null idxDir");
		_idxDir=idxDir;

		_searchIdxMgr=new SearchIndexManager<R>(_idxDir,indexReaderDecorator);
		_realtimeIndexing=rtIndexing;
		_interpreter=interpreter;
		
		_analyzer= analyzer== null ? new StandardAnalyzer() : analyzer;
		_similarity=similarity==null ? new DefaultSimilarity() : similarity;
		
		
		 // build event call back for batch
	    
		_diskLoader=new DiskLuceneIndexDataLoader<R>(_analyzer,_similarity,_searchIdxMgr);

		_diskLoader.setOptimizeScheduler(new DefaultOptimizeScheduler());
	    
		_lsnrList = Collections.synchronizedList(new LinkedList<IndexingEventListener>());
		
	      if (_realtimeIndexing)
	      {
	    	  _ramLoader=new RAMLuceneIndexDataLoader<R>(_analyzer,_similarity,_searchIdxMgr);
	    	  //_batchedDiskLoader=new CopyingIndexDataLoader<R>(_diskLoader,Math.max(1,batchSize),100000,batchDelay,_searchIdxMgr,_lsnrList);
	    	  _batchedDiskLoader=new BatchedIndexDataLoader<R,Indexable>(_diskLoader,Math.max(1,batchSize),100000,batchDelay,_searchIdxMgr,_lsnrList);
	      }
	      else
	      {
	    	  _ramLoader=null;
	    	  _batchedDiskLoader=new BatchedIndexDataLoader<R,Indexable>(_diskLoader,Math.max(1,batchSize),100000,batchDelay,_searchIdxMgr,_lsnrList);
	      }
	      
	      _delegateConsumer = new DelegateIndexDataConsumer<V>(_batchedDiskLoader,_ramLoader,_interpreter);
	      super.setBatchSize(100); // realtime memory batch size
	      super.setDataConsumer(_delegateConsumer);
	}
	
	public void addIndexingEventListener(IndexingEventListener lsnr){
		_lsnrList.add(lsnr);
	}
	
	public OptimizeScheduler getOptimizeScheduler(){
		return _diskLoader.getOptimizeScheduler();
	}
	
	public void setOptimizeScheduler(OptimizeScheduler scheduler){
		if (scheduler!=null){
			_diskLoader.setOptimizeScheduler(scheduler);
		}
	}
	
	public long getCurrentDiskVersion() throws IOException
	{
		return _searchIdxMgr.getCurrentDiskVersion();
	}
	
	public Analyzer getAnalyzer()
	{
		return _analyzer;
	}
	
	public Similarity getSimilarity()
	{
		return _similarity;
	}
	
	public void start()
	{
		log.info("starting zoie...");
		_batchedDiskLoader.start();
        super.start();
		log.info("zoie started...");
	}
	
	public void shutdown()
	{
		log.info("shutting down zoie...");
		_batchedDiskLoader.shutdown();
        super.stop();
		log.info("zoie shutdown successfully.");
		
	}
	
	public void refreshDiskReader() throws IOException
	{
		_searchIdxMgr.refreshDiskReader();
	}
	/**
	 * Flush the memory index into disk.
	 * @throws ZoieException 
	 */
	public void flushEvents(long timeout) throws ZoieException
	{
	  super.flushEvents(timeout);
	  if (_batchedDiskLoader!=null)
	  {
		  _batchedDiskLoader.flushEvents(timeout);
	  }
	}
	
	public boolean isReadltimeIndexing()
	{
		return _realtimeIndexing;
	}
	
	public List<R> getIndexReaders() throws IOException
	{
		return _searchIdxMgr.getIndexReaders();
	}
	
	public void purgeIndex() throws IOException
	{
		FileUtil.rmDir(_idxDir);
	}
	
    public void setRealtimeThreshold(long threshold)
    {
      _searchIdxMgr.setRealtimeThreshold(threshold);
    }
    public long getRealtimeThreshold()
    {
      return _searchIdxMgr.getRealtimeThreshold();
    }
    public boolean isRealtimeSuspended()
    {
      return _searchIdxMgr.isRealtimeSuspended();
    }
	
    public int getCurrentMemBatchSize()
    {
      return getCurrentBatchSize(); 
    }
    
    public int getCurrentDiskBatchSize()
    {
      return _batchedDiskLoader.getCurrentBatchSize(); 
    }
    
    public void setMaxBatchSize(int maxBatchSize) {
	  _batchedDiskLoader.setMaxBatchSize(maxBatchSize);
	}
	
    
	public ZoieSystemAdminMBean getAdminMBean()
	{
		return new MyZoieSystemAdmin();
	}
	
	private class MyZoieSystemAdmin implements ZoieSystemAdminMBean
	{
		public void refreshDiskReader() throws IOException{
			ZoieSystem.this.refreshDiskReader();
		}
		
		public long getBatchDelay() {
			return ZoieSystem.this._batchedDiskLoader.getDelay();
		}

		public int getBatchSize() {
			return ZoieSystem.this._batchedDiskLoader.getBatchSize();
		}

		public long getCurrentDiskVersion() throws IOException
		{
			return ZoieSystem.this.getCurrentDiskVersion();
		}

		public int getDiskIndexSize() {
			return ZoieSystem.this._searchIdxMgr.getDiskIndexSize();
		}

		public String getDiskIndexerStatus() {
			return String.valueOf(ZoieSystem.this._searchIdxMgr.getDiskIndexerStatus());
		}

		public Date getLastDiskIndexModifiedTime() {
			File directoryFile = new File(ZoieSystem.this._idxDir, IndexReaderDispenser.INDEX_DIRECTORY);
			return new Date(directoryFile.lastModified());		
		}
		
		public String getIndexDir()
		{
			return ZoieSystem.this._idxDir.getAbsolutePath();
		}

		public Date getLastOptimizationTime() {
			return new Date(ZoieSystem.this._diskLoader.getLastTimeOptimized());
		}

		public int getMaxBatchSize() {
			return ZoieSystem.this._batchedDiskLoader.getMaxBatchSize();
		}
		
		public boolean isRealtime(){
			return ZoieSystem.this.isReadltimeIndexing();
		}

		public int getRamAIndexSize() {
			return ZoieSystem.this._searchIdxMgr.getRamAIndexSize();
		}

		public long getRamAVersion() {
			return ZoieSystem.this._searchIdxMgr.getRamAVersion();
		}

		public int getRamBIndexSize() {
			return ZoieSystem.this._searchIdxMgr.getRamBIndexSize();
		}

		public long getRamBVersion() {
			return ZoieSystem.this._searchIdxMgr.getRamBVersion();
		}

		public void optimize(int numSegs) throws IOException {
			ZoieSystem.this._diskLoader.optimize(numSegs);
		}
		
		public void flushToDiskIndex(long timeout) throws ZoieException
		{
			log.info("flushing to disk with timeout: "+timeout+"ms");
			ZoieSystem.this.flushEvents(timeout);
			log.info("all events flushed to disk");
		}

		public void setBatchDelay(long batchDelay) {
			ZoieSystem.this._batchedDiskLoader.setDelay(batchDelay);
		}

		public void setBatchSize(int batchSize) {
			ZoieSystem.this._batchedDiskLoader.setBatchSize(batchSize);
		}

		public void setMaxBatchSize(int maxBatchSize) {
			ZoieSystem.this.setMaxBatchSize(maxBatchSize);
		}

		public void purgeIndex() throws IOException{
			ZoieSystem.this.purgeIndex();
		}

		public void expungeDeletes() throws IOException{
			ZoieSystem.this._diskLoader.expungeDeletes();
		}

		public int getMaxMergeDocs() {
			return ZoieSystem.this._searchIdxMgr.getMaxMergeDocs();
		}

		public int getMergeFactor() {
			return ZoieSystem.this._searchIdxMgr.getMergeFactor();
		}

		public void setMaxMergeDocs(int maxMergeDocs) {
			ZoieSystem.this._searchIdxMgr.setMaxMergeDocs(maxMergeDocs);
		}

		public void setMergeFactor(int mergeFactor) {
			ZoieSystem.this._searchIdxMgr.setMergeFactor(mergeFactor);
		}

		public boolean isUseCompoundFile() {
			return ZoieSystem.this._searchIdxMgr.isUseCompoundFile();
		}

		public void setUseCompoundFile(boolean useCompoundFile) {
			ZoieSystem.this._searchIdxMgr.setUseCompoundFile(useCompoundFile);
		}
		
        public void setRealtimeThreshold(long threshold)
        {
          ZoieSystem.this.setRealtimeThreshold(threshold);
        }
        
        public long getRealtimeThreshold()
        {
          return ZoieSystem.this.getRealtimeThreshold();
        }
        
        public boolean isRealtimeSuspended()
        {
          return ZoieSystem.this.isRealtimeSuspended();
        }
        
        public int getCurrentMemBatchSize()
        {
          return ZoieSystem.this.getCurrentMemBatchSize(); 
        }
        
        public int getCurrentDiskBatchSize()
        {
          return ZoieSystem.this.getCurrentDiskBatchSize(); 
        }
	}
}
