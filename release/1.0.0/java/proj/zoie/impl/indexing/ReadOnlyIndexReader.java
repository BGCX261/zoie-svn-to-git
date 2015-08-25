package proj.zoie.impl.indexing;

import java.io.IOException;

import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;

public class ReadOnlyIndexReader extends FilterIndexReader {
	  public ReadOnlyIndexReader(IndexReader in)
	  {
	    super(in);
	  }
	
	  @Override
	  protected final void doCommit()
	      throws IOException
	  {
	    throw new IOException("Only read access is allowed.");
	  }
	
	  @Override
	  protected final void doDelete(int n)
	      throws IOException
	  {
	    throw new IOException("Only read access is allowed.");
	  }
	
	  @Override
	  protected final void doSetNorm(int d, String f, byte b)
	      throws IOException
	  {
	    throw new IOException("Only read access is allowed.");
	  }
	
	  @Override
	  protected final void doUndeleteAll()
	      throws IOException
	  {
	    throw new IOException("Only read access is allowed.");
	  }
}
