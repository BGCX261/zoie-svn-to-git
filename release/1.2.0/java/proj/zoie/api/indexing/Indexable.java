package proj.zoie.api.indexing;

import org.apache.lucene.document.Document;

public interface Indexable {
	/**
	 * document ID field name
	*/
	public static final String DOCUMENT_ID_FIELD = "id";
	  
	public static final String DOCUMENT_ID_PAYLOAD_FIELD="_ID";
	  
	int getUID();
	boolean isDeleted();
	boolean isSkip();
	Document[] buildDocuments();
}
