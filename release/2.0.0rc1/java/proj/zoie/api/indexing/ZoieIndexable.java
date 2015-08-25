package proj.zoie.api.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

public interface ZoieIndexable{
	/**
	 * document ID field name
	 * @deprecated this field should no longer be used
	*/
	public static final String DOCUMENT_ID_FIELD = "id";
	  
	/**
	 * @deprecated please see {@link AbstractZoieIndexable#DOCUMENT_ID_PAYLOAD_FIELD}
	 */
	public static final String DOCUMENT_ID_PAYLOAD_FIELD="_ID";
	
	public static final class IndexingReq{
		private final Document _doc;
		private final Analyzer _analyzer;
		
		public IndexingReq(Document doc){
			this(doc,null);
		}
		
		public IndexingReq(Document doc,Analyzer analyzer){
			_doc = doc;
			_analyzer = analyzer;
		}
		
		public Document getDocument(){
			return _doc;
		}
		
		public Analyzer getAnalyzer(){
			return _analyzer;
		}
	}
	
	long getUID();
	boolean isDeleted();
	boolean isSkip();
	
	IndexingReq[] buildIndexingReqs();
}
