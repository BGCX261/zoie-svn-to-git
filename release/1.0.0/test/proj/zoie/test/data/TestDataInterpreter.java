package proj.zoie.test.data;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

import proj.zoie.api.indexing.Indexable;
import proj.zoie.api.indexing.IndexableInterpreter;

public class TestDataInterpreter implements IndexableInterpreter<String> {

	public Indexable interpret(final String src) {
		String[] parts=src.split(" ");
		final int id=Integer.parseInt(parts[parts.length-1]);
		return new Indexable(){
			public Document[] buildDocuments() {
				Document doc=new Document();
				doc.add(new Field("contents",src,Store.NO,Index.TOKENIZED));
				return new Document[]{doc};
			}

			public int getUID() {
				return id;
			}

			public boolean isDeleted() {
				return false;
			}

			public boolean isSkip() {
				return false;
			}
		};
	}

}
