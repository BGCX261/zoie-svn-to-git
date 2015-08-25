package proj.zoie.api.indexing;


public abstract class AbstractZoieIndexable implements ZoieIndexable {
	public static final String DOCUMENT_ID_PAYLOAD_FIELD="_ID";
	
	public abstract IndexingReq[] buildIndexingReqs();

	abstract public long getUID();

	abstract public boolean isDeleted();

	public boolean isSkip(){
		return false;
	}

}
