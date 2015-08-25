package proj.zoie.api;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;

import proj.zoie.api.indexing.Indexable;
import proj.zoie.impl.indexing.ReadOnlyIndexReader;

public class ZoieIndexReader extends ReadOnlyIndexReader {
	private static final Term UID_TERM = new Term(Indexable.DOCUMENT_ID_PAYLOAD_FIELD, "_UID");
	private int[] _uidArray;
	private static byte[] payloadBuffer=new byte[4];		// four bytes for an int
	
	private IntSet _delSet;
	private int _minUID;
	private int _maxUID;
	
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
		   Field f=doc.getField(UID_TERM.field());
		   if (f==null)
		   {
			   f=new Field(UID_TERM.field(), singlePayloadTokenStream);
			   doc.add(f);
		   }
		   else{
			   f.setValue(singlePayloadTokenStream);
		   }
		   f=null;
		   f=doc.getField(Indexable.DOCUMENT_ID_FIELD);
		   if (f==null)
		   {
			   f=new Field(Indexable.DOCUMENT_ID_FIELD,String.valueOf(id),Store.NO,Index.UN_TOKENIZED);
			   doc.add(f);
		   }
		   else
		   {
			   f.setValue(String.valueOf(id));
		   }
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
		TermPositions tp=null;
		int uid;
		try
		{
          tp = reader.termPositions(UID_TERM);
          while (tp.next())
          {
    	      assert tp.doc() < maxDoc;
    	      tp.nextPosition();
    	      tp.getPayload(payloadBuffer, 0);
    	      uid=bytesToInt(payloadBuffer);
    	      if (uid < _minUID) _minUID=uid;
    	      if (uid > _maxUID) _maxUID=uid;
    	      _uidArray[tp.doc()] = uid;
    	  }
		}
		finally
		{
          if (tp!=null)
          {
        	  tp.close();
          }
		}
	}
	
	public ZoieIndexReader(IndexReader in) throws IOException{
		super(in);
		_delSet=null;
		_minUID=Integer.MAX_VALUE;
		_maxUID=0;
		init(in);
	}
	
	public void setDelSet(IntSet delSet)
	{
		_delSet=delSet;
	}

	
	@Override
	public boolean hasDeletions() {
		return super.hasDeletions() || (_delSet!=null && _delSet.size() > 0);
	}

	@Override
	public boolean isDeleted(int n) {
		if (isDuplicate(getUID(n))) return true;
		return super.isDeleted(n);
	}
	
	public boolean isDuplicate(int uid)
	{
		return _delSet!=null && _delSet.contains(uid);
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
}
