package proj.zoie.api;

import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.store.Directory;

import proj.zoie.api.indexing.IndexReaderDecorator;

public abstract class ZoieIndexReader<R extends IndexReader> extends FilterIndexReader {
	public static final long DELETED_UID = Long.MIN_VALUE;
	
	protected ThreadLocal<LongSet> _delSet;
	protected long _minUID;
	protected long _maxUID;
	protected LongSet _modifiedSet;
	protected boolean _noDedup = false;
    protected final IndexReaderDecorator<R> _decorator;
	
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r) throws IOException{
		return open(r,null);
	}
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r,IndexReaderDecorator<R> decorator) throws IOException{
		return new ZoieMultiReader<R>(r,decorator);
	}
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(Directory dir,IndexReaderDecorator<R> decorator) throws IOException{
		IndexReader r = IndexReader.open(dir, true);
		// load zoie reader
		try{
			return open(r,decorator);
		}
		catch(IOException ioe){
			if (r!=null){
				r.close();
			}
			throw ioe;
		}
	}
		
	protected ZoieIndexReader(IndexReader in,IndexReaderDecorator<R> decorator) throws IOException
	{
		super(in);
		_decorator = decorator;
		_delSet=new ThreadLocal<LongSet>();
		_minUID=Long.MAX_VALUE;
		_maxUID=0;
	}
	
	abstract public List<R> getDecoratedReaders() throws IOException;
	
	public IndexReader getInnerReader(){
		return in;
	}
	
	public void setDelSet(LongSet delSet)
	{
		_delSet.set(delSet);
	}

	public void setModifiedSet(LongSet modSet)
	{
	  _modifiedSet = modSet;
	}
	
	public LongSet getModifiedSet()
	{
	  return _modifiedSet;
	}
	
	@Override
	public boolean hasDeletions()
	{
	  if(!_noDedup)
	  {
		LongSet delSet = _delSet.get();
	    if(delSet != null && delSet.size() > 0) return true;
	  }
	  return in.hasDeletions();
	}
	
	protected abstract boolean hasIndexDeletions();
	
	public boolean hasDuplicates()
	{
		LongSet delSet = _delSet.get();
		return (delSet!=null && delSet.size() > 0);
	}

	@Override
	abstract public boolean isDeleted(int docid);
	
	public boolean isDuplicate(int uid)
	{
	  LongSet delSet = _delSet.get();
	  return delSet!=null && delSet.contains(uid);
	}
	
	public LongSet getDelSet()
	{
	  return _delSet.get();
	}
	
	public long getMinUID()
	{
		return _minUID;
	}
	
	public long getMaxUID()
	{
		return _maxUID;
	}

	abstract public long getUID(int docid);
	
	abstract public DocIDMapper getDocIDMaper();
	
	public void setNoDedup(boolean noDedup)
	{
	  _noDedup = noDedup;
	}

	@Override
	abstract public ZoieIndexReader<R>[] getSequentialSubReaders();
	
	@Override
	abstract public TermDocs termDocs() throws IOException;
	
	@Override
	abstract public TermPositions termPositions() throws IOException;
	
}
