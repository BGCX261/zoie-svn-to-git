package proj.zoie.api.indexing;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;

public interface IndexReaderDecorator<R extends IndexReader>
{
	R decorate(ZoieIndexReader<?> indexReader) throws IOException;
	R redecorate(IndexReader decorated,ZoieIndexReader<?> copy) throws IOException;
}
