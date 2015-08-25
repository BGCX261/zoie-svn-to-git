package proj.zoie.api.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

public interface ZoieIndexable extends Indexable {
	
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
	
	IndexingReq[] buildIndexingReqs();
}
