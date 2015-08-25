package proj.zoie.api.indexing;

import org.apache.lucene.document.Document;

public abstract class AbstractZoieIndexable implements ZoieIndexable {
	public static final String DOCUMENT_ID_PAYLOAD_FIELD="_ID";
	
	public IndexingReq[] buildIndexingReqs() {
		Document[] docs = buildDocuments();
		IndexingReq[] reqs = new IndexingReq[docs.length];
		for (int i=0;i<reqs.length;++i){
			reqs[i] = new IndexingReq(docs[i],null);
		}
		return reqs;
	}

	abstract public Document[] buildDocuments();

	abstract public int getUID();

	abstract public boolean isDeleted();

	abstract public boolean isSkip();

}
