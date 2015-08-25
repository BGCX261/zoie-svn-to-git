package proj.zoie.impl.indexing;

import java.io.File;
import java.io.IOException;
import java.util.Date;
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
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.Indexable;
import proj.zoie.api.indexing.IndexableInterpreter;
import proj.zoie.impl.indexing.internal.BatchedIndexDataLoader;
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
	private BatchedIndexDataLoader<Indexable>  _batchedDiskLoader;
	private DiskLuceneIndexDataLoader<R> _diskLoader;
	private DelegateIndexDataConsumer<V> _delegateConsumer;
	
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
		
	      
	      if (_realtimeIndexing)
	      {
	    	  _ramLoader=new RAMLuceneIndexDataLoader<R>(_analyzer,_similarity,_searchIdxMgr);
	      }
	      else
	      {
	    	  _ramLoader=null;
	      }
	      
	      _batchedDiskLoader=new BatchedIndexDataLoader<Indexable>(_diskLoader,Math.max(1,batchSize),100000,batchDelay)
	      {
	        protected List<DataEvent<Indexable>> getBatchList()
	        {
	          // atomically change the status and get the batch list
	          // (this method is called in the block synchronized on BatchIndexDataLoader)
	          _searchIdxMgr.setDiskIndexerStatus(SearchIndexManager.Status.Working);
	          return super.getBatchList();
	        }
	      };
	      
	      _delegateConsumer = new DelegateIndexDataConsumer<V>(_batchedDiskLoader,_ramLoader,_interpreter);
	      super.setBatchSize(100); // realtime memory batch size
	      super.setDataConsumer(_delegateConsumer);
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
	
	public R getDiskIndexReader() throws IOException
	{
		return _searchIdxMgr.getDiskIndexReader();
	}
	
	public void purgeIndex() throws IOException
	{
		FileUtil.rmDir(_idxDir);
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
			return ZoieSystem.this._searchIdxMgr.getCurrentDiskVersion();
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

		public long getOptimizationDuration() {
			return ZoieSystem.this._diskLoader.getOptimizationDuration();
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

		public void optimize() throws IOException {
			ZoieSystem.this._diskLoader.optimize();
		}
		
		public void flushToDiskIndex(long timeout) throws ZoieException
		{
			ZoieSystem.this.flushEvents(timeout);
		}

		public void setBatchDelay(long batchDelay) {
			ZoieSystem.this._batchedDiskLoader.setDelay(batchDelay);
		}

		public void setBatchSize(int batchSize) {
			ZoieSystem.this._batchedDiskLoader.setBatchSize(batchSize);
		}

		public void setMaxBatchSize(int maxBatchSize) {
			ZoieSystem.this._batchedDiskLoader.setMaxBatchSize(maxBatchSize);
		}
		

		public void setOptimizationDuration(long duration) {
			ZoieSystem.this._diskLoader.setOptimizationDuration(duration);
		}

		public void purgeIndex() throws IOException{
			ZoieSystem.this.purgeIndex();
		}
		
	}
}
