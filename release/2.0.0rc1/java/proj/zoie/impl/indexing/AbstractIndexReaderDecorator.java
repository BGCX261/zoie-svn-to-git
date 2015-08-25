package proj.zoie.impl.indexing;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public abstract class AbstractIndexReaderDecorator<R extends IndexReader> implements IndexReaderDecorator<R> {

	public abstract R decorate(ZoieIndexReader<?> indexReader) throws IOException;

	public R redecorate(IndexReader decorated, ZoieIndexReader<?> copy) throws IOException {
		return decorate(copy);
	}

}
