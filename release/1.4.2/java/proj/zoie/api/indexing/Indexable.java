package proj.zoie.api.indexing;

import org.apache.lucene.document.Document;

/**
 * @deprecated Please use {@link ZoieIndexable}
 * @author john
 *
 */
public interface Indexable {
	/**
	 * document ID field name
	 * @deprecated this field should no longer be used
	*/
	public static final String DOCUMENT_ID_FIELD = "id";
	  
	/**
	 * @deprecated please see {@link AbstractZoieIndexable#DOCUMENT_ID_PAYLOAD_FIELD}
	 */
	public static final String DOCUMENT_ID_PAYLOAD_FIELD="_ID";
	  
	int getUID();
	boolean isDeleted();
	boolean isSkip();
	Document[] buildDocuments();
}
