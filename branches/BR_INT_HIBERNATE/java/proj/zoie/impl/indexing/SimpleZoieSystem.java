package proj.zoie.impl.indexing;

import java.io.File;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.indexing.IndexableInterpreter;

public class SimpleZoieSystem<V> extends ZoieSystem<IndexReader,V> {

	public SimpleZoieSystem(File idxDir, IndexableInterpreter<V> interpreter,int batchSize, long batchDelay) {
		super(idxDir, interpreter, new DefaultIndexReaderDecorator(), null,null,batchSize, batchDelay, true);
	}

}
