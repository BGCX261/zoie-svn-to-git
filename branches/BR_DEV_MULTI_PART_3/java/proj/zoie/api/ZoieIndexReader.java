package proj.zoie.api;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.impl.DocIDMapperImpl;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieIndexReader extends FilterIndexReader {
	private static final Term UID_TERM = new Term(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD, "_UID");
	public static final int DELETED_UID = Integer.MIN_VALUE;
	private int[] _uidArray;
	
	private ThreadLocal<IntSet> _delSet;
	private int _minUID;
	private int _maxUID;
	private IntSet _modifiedSet;
	private DocIDMapper _docIdMapper;
	private boolean _noDedup = false;
	
	private static class SinglePayloadTokenStream extends TokenStream {
		   private Token token = new Token(UID_TERM.text(), 0, 0);
		   private byte[] buffer = new byte[4];
		   private boolean returnToken = false;

		   void setUID(int uid) {
		     buffer[0] = (byte) (uid);
		     buffer[1] = (byte) (uid >> 8);
		     buffer[2] = (byte) (uid >> 16);
		     buffer[3] = (byte) (uid >> 24);
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
	
	public static void fillDocumentID(Document doc,int id)
	{
		  SinglePayloadTokenStream singlePayloadTokenStream = new SinglePayloadTokenStream();
		  singlePayloadTokenStream.setUID(id);
		  doc.add(new Field(UID_TERM.field(), singlePayloadTokenStream)); 
	  }
	
	private static int bytesToInt(byte[] bytes) 
	 {
	   return ((bytes[3] & 0xFF) << 24) | ((bytes[2] & 0xFF) << 16)
	   | ((bytes[1] & 0xFF) <<  8) |  (bytes[0] & 0xFF);
	 }
	
	private void init(IndexReader reader) throws IOException
	{
		int maxDoc = reader.maxDoc();
		_uidArray = new int[maxDoc]; 
		TermPositions tp = null;
		byte[] payloadBuffer = new byte[4];       // four bytes for an int
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
            int uid = bytesToInt(payloadBuffer);
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
	
	public ZoieIndexReader(IndexReader in) throws IOException
	{
		super(in);
		_delSet=new ThreadLocal<IntSet>();
		_minUID=Integer.MAX_VALUE;
		_maxUID=0;
		init(in);
	}
	
	public IndexReader getInnerReader(){
		return this.in;
	}
	
	public void setDelSet(IntSet delSet)
	{
		_delSet.set(delSet);
	}

	public void setModifiedSet(IntSet modSet)
	{
	  _modifiedSet = modSet;
	}
	
	public IntSet getModifiedSet()
	{
	  return _modifiedSet;
	}
	
	@Override
	public boolean hasDeletions()
	{
	  if(!_noDedup)
	  {
	    IntSet delSet = _delSet.get();
	    if(delSet != null && delSet.size() > 0) return true;
	  }
	  return in.hasDeletions();
	}
	
	public boolean hasDuplicates()
	{
		IntSet delSet = _delSet.get();
		return (delSet!=null && delSet.size() > 0);
	}

	@Override
	public boolean isDeleted(int docid)
	{
	  if(!_noDedup)
	  {
	    IntSet delSet = _delSet.get();
	    if(delSet != null && delSet.contains(_uidArray[docid])) return true;
	  }
	  return in.isDeleted(docid);
	}
	
	public boolean isDuplicate(int uid)
	{
	  IntSet delSet = _delSet.get();
	  return delSet!=null && delSet.contains(uid);
	}
	
	public IntSet getDelSet()
	{
	  return _delSet.get();
	}
	
	public int[] getUIDArray()
	{
		return _uidArray;
	}
	
	public int getMinUID()
	{
		return _minUID;
	}
	
	public int getMaxUID()
	{
		return _maxUID;
	}

	public int getUID(int docid)
	{
		return _uidArray[docid];
	}
	
	public DocIDMapper getDocIDMaper()
	{
	  if(_docIdMapper == null)
	  {
	    _docIdMapper = new DocIDMapperImpl(_uidArray);
	  }
	  return _docIdMapper;
	}
	
	public void setNoDedup(boolean noDedup)
	{
	  _noDedup = noDedup;
	}
	
	@Override
	public TermDocs termDocs() throws IOException
	{
	  TermDocs td = in.termDocs();
	  if(_noDedup) return td;
	  
	  IntSet delSet = _delSet.get();
	  if(td == null || delSet == null || delSet.size() == 0) return td;
      
      return new ZoieTermDocs(td, delSet);
	}
	
	@Override
	public TermPositions termPositions() throws IOException
	{
	  TermPositions tp = in.termPositions();
      if(_noDedup) return tp;
      
      IntSet delSet = _delSet.get();
      if(tp == null || delSet == null || delSet.size() == 0) return tp;
      
      return new ZoieTermPositions(tp, delSet);
	}
	
	private class ZoieTermDocs extends FilterTermDocs
	{
	  final IntSet _termDelSet;

	  public ZoieTermDocs(TermDocs in, IntSet delSet)
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
	
	private class ZoieTermPositions extends ZoieTermDocs implements TermPositions
	{
	  final TermPositions _tp;
	  
	  public ZoieTermPositions(TermPositions in, IntSet delSet)
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
}
