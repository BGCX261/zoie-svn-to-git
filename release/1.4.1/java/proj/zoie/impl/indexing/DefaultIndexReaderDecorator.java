package proj.zoie.impl.indexing;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class DefaultIndexReaderDecorator implements IndexReaderDecorator<IndexReader> {

	public IndexReader decorate(ZoieIndexReader indexReader) throws IOException {
		return indexReader;
	}

}
