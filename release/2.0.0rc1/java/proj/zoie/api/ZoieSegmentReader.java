package proj.zoie.api;

import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;

import proj.zoie.api.impl.DocIDMapperImpl;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieSegmentReader<R extends IndexReader> extends ZoieIndexReader<R>{
	static final char[] fieldname="_UID".toCharArray();
	static final Term UID_TERM = new Term(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD, String.valueOf(fieldname));
    private R _decoratedReader;
    private long[] _uidArray;
	private DocIDMapper _docIdMapper;
    
    static class SinglePayloadTokenStream extends TokenStream {
    	private Token token = new Token(fieldname, 0,fieldname.length,0, 0);
        private byte[] buffer = new byte[8];
        private boolean returnToken = false;

        void setUID(long uid) {
          buffer[0] = (byte) (uid);
          buffer[1] = (byte) (uid >> 8);
          buffer[2] = (byte) (uid >> 16);
          buffer[3] = (byte) (uid >> 24);
          buffer[4] = (byte) (uid >> 32);
          buffer[5] = (byte) (uid >> 40);
          buffer[6] = (byte) (uid >> 48);
          buffer[7] = (byte) (uid >> 56);
          token.setPayload(new Payload(buffer));
          returnToken = true;
        }

        public Token next() throws IOException {
          if (returnToken) {
            returnToken = false;
            return token;
          } else {
            return null;
          }
        }
		  
    }

	public static void fillDocumentID(Document doc,long id)
	{
	  SinglePayloadTokenStream singlePayloadTokenStream = new SinglePayloadTokenStream();
	  singlePayloadTokenStream.setUID(id);
      doc.add(new Field(ZoieSegmentReader.UID_TERM.field(), singlePayloadTokenStream)); 
	}

	public ZoieSegmentReader(IndexReader in, IndexReaderDecorator<R> decorator)
			throws IOException {
		super(in,decorator);
		if (!(in instanceof SegmentReader)){
			throw new IllegalStateException("ZoieSegmentReader can only be constucted from "+SegmentReader.class);
		}
		init(in);
		_decoratedReader = (decorator == null ? null : decorator.decorate(this));
	}
	
	ZoieSegmentReader(ZoieSegmentReader<R> copyFrom,IndexReader innerReader) throws IOException{
		super(innerReader,copyFrom._decorator);
		_uidArray = copyFrom._uidArray;
		_maxUID = copyFrom._maxUID;
		_minUID = copyFrom._minUID;
		_noDedup = copyFrom._noDedup;
		_docIdMapper = copyFrom._docIdMapper;
		
		if (copyFrom._decorator == null){
			_decoratedReader = null;
		}
		else{
			_decoratedReader = copyFrom._decorator.redecorate(copyFrom._decoratedReader, this);
		}
	}
	
	public R getDecoratedReader(){
		return _decoratedReader;
	}
	
	@Override
	public List<R> getDecoratedReaders()
    {
	  ArrayList<R> list = new ArrayList<R>(1);
	  if (_decoratedReader!=null){
	    list.add(_decoratedReader);
	  }
	  return list;
    }
	
	private void init(IndexReader reader) throws IOException
	{
		int maxDoc = reader.maxDoc();
		_uidArray = new long[maxDoc]; 
		TermPositions tp = null;
		byte[] payloadBuffer = new byte[8];       // four bytes for a long
		try
		{
          tp = reader.termPositions(UID_TERM);
          int idx = 0;
          while (tp.next())
          {
            int doc = tp.doc();
            assert doc < maxDoc;
            
            while(idx < doc) _uidArray[idx++] = DELETED_UID; // fill the gap
            
            tp.nextPosition();
            tp.getPayload(payloadBuffer, 0);
            long uid = bytesToLong(payloadBuffer);
            if(uid < _minUID) _minUID = uid;
            if(uid > _maxUID) _maxUID = uid;
            _uidArray[idx++] = uid;
    	  }
          while(idx < maxDoc) _uidArray[idx++] = DELETED_UID; // fill the gap
		}
		finally
		{
          if (tp!=null)
          {
        	  tp.close();
          }
		}
	}
	
	private static long bytesToLong(byte[] bytes){
        return ((bytes[7] & 0xFF) << 56) | ((bytes[6] & 0xFF) << 48) | ((bytes[5] & 0xFF) << 40) | ((bytes[4] & 0xFF) << 32) | ((bytes[3] & 0xFF) << 24) | ((bytes[2] & 0xFF) << 16)
           | ((bytes[1] & 0xFF) <<  8) |  (bytes[0] & 0xFF);
	}
	
	@Override
	public long getUID(int docid)
	{
		return _uidArray[docid];
	}

	public long[] getUIDArray()
	{
		return _uidArray;
	}

	@Override
	protected boolean hasIndexDeletions(){
		return in.hasDeletions();
	}
	
	@Override
	public boolean isDeleted(int docid)
	{
	  if(!_noDedup)
	  {
		LongSet delSet = _delSet.get();
	    if(delSet != null && delSet.contains(_uidArray[docid])) return true;
	  }
	  return in.isDeleted(docid);
	}
	
	public DocIDMapper getDocIDMaper()
	{
	  if(_docIdMapper == null)
	  {
	    _docIdMapper = new DocIDMapperImpl(_uidArray);
	  }
	  return _docIdMapper;
	}
	
	
	@Override
	public TermDocs termDocs(Term term) throws IOException {
		 ensureOpen();
		 TermDocs td = in.termDocs(term);
		 if(_noDedup) return td;
		  
		 LongSet delSet = _delSet.get();
		 if(td == null || delSet == null || delSet.size() == 0) return td;
	     return new ZoieTermDocs(td, delSet);
	}

	@Override
	public TermDocs termDocs() throws IOException
	{
	  ensureOpen();
	  TermDocs td = in.termDocs();
	  if(_noDedup) return td;
	  
	  LongSet delSet = _delSet.get();
	  if(td == null || delSet == null || delSet.size() == 0) return td;
      
      return new ZoieTermDocs(td, delSet);
	}
	
	@Override
	public TermPositions termPositions(Term term) throws IOException {
		ensureOpen();
		  TermPositions tp = in.termPositions(term);
	      if(_noDedup) return tp;
	      
	      LongSet delSet = _delSet.get();
	      if(tp == null || delSet == null || delSet.size() == 0) return tp;
	      
	      return new ZoieTermPositions(tp, delSet);
	}

	@Override
	public TermPositions termPositions() throws IOException
	{
	  ensureOpen();
	  TermPositions tp = in.termPositions();
      if(_noDedup) return tp;
      
      LongSet delSet = _delSet.get();
      if(tp == null || delSet == null || delSet.size() == 0) return tp;
      
      return new ZoieTermPositions(tp, delSet);
	}
	

	class ZoieTermDocs extends FilterTermDocs
	{
	  final LongSet _termDelSet;

	  public ZoieTermDocs(TermDocs in, LongSet delSet)
	  {
	    super(in);
	    _termDelSet = delSet;
	  }
	  
	  public boolean next() throws IOException
	  {
	    while(in.next())
	    {
	      if(!_termDelSet.contains(_uidArray[in.doc()])) return true;
	    }
	    return false;
	  }
	  
	  public int read(final int[] docs, final int[] freqs) throws IOException
	  {
	    int i = 0;
	    while(i < docs.length)
	    {
	      if(!in.next()) return i;
	      
	      int doc = in.doc();
	      if(!_termDelSet.contains(_uidArray[doc]))
	      {
	        docs[i] = doc;
	        freqs[i] = in.freq();
	        i++;
	      }
	    }
	    return i;
	  }
	  public boolean skipTo(int i) throws IOException
	  {
	    if(!in.skipTo(i)) return false;
	    if(!_termDelSet.contains(_uidArray[in.doc()])) return true;
	    
        return next();
	  }
	}
	
	class ZoieTermPositions extends ZoieTermDocs implements TermPositions
	{
	  final TermPositions _tp;
	  
	  public ZoieTermPositions(TermPositions in, LongSet delSet)
	  {
	    super(in, delSet);
	    _tp = (TermPositions)in;
	  }
	  
	  public int nextPosition() throws IOException
	  {
	    return _tp.nextPosition();
	  }

	  public int getPayloadLength()
	  {
	    return _tp.getPayloadLength();
	  }

	  public byte[] getPayload(byte[] data, int offset) throws IOException
	  {
	    return _tp.getPayload(data, offset);
	  }
	  
	  public boolean isPayloadAvailable()
	  {
	    return _tp.isPayloadAvailable();
	  }
	}

	@Override
	public ZoieIndexReader<R>[] getSequentialSubReaders() {
		return null;
	}
	
	public String getSegmentName(){
		return ((SegmentReader)in).getSegmentName();
	}

	@Override
	protected void doClose() throws IOException {
		
	}

	@Override
	public synchronized void decRef() throws IOException {
		
	}
}
