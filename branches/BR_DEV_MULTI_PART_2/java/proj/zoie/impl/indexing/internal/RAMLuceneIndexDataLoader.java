package proj.zoie.impl.indexing.internal;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

public class RAMLuceneIndexDataLoader<R extends IndexReader> extends LuceneIndexDataLoader<R> {

	public RAMLuceneIndexDataLoader(Analyzer analyzer, Similarity similarity,SearchIndexManager<R> idxMgr) {
		super(analyzer, similarity,idxMgr);
	}

	@Override
	protected BaseSearchIndex<R> getSearchIndex() {
		return _idxMgr.getCurrentWritableMemoryIndex();
	}

}
