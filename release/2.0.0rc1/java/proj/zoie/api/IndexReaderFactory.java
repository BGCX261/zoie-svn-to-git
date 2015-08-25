package proj.zoie.api;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

public interface IndexReaderFactory<R extends IndexReader> {
	List<R> getIndexReaders() throws IOException;
	Analyzer getAnalyzer();
	void returnIndexReaders(List<R> r);
}
