package proj.zoie.hibernate.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.index.IndexReader;

import proj.zoie.hibernate.cfg.ZoieSearchConfiguration;
import proj.zoie.hibernate.index.HibernateIndexableInterpreter;
import proj.zoie.impl.indexing.FileDataProvider;
import proj.zoie.impl.indexing.SimpleZoieSystem;
import proj.zoie.impl.indexing.ZoieSystem;

public class TestClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		File dataDir = new File(args[0]);
		File idxDir = new File(args[1]);
		
		Map<String,Class<?>> map = new HashMap<String,Class<?>>();
		map.put("entity",TestEntity.class);
		
		ZoieSearchConfiguration zoieHibernateConfig = new ZoieSearchConfiguration(new Properties(),map);
		HibernateIndexableInterpreter<TestEntity> interpreter = HibernateIndexableInterpreter.getHibernateIndexableInterpreter(zoieHibernateConfig, TestEntity.class);
		ZoieSystem<IndexReader, TestEntity> indexer = new SimpleZoieSystem<TestEntity>(idxDir,interpreter,10,100);
		indexer.start();
		
		FileDataProvider fileProvider = new FileDataProvider(dataDir);
		
		TestEntityDataConsumer entityConsumer  = new TestEntityDataConsumer(indexer);
		fileProvider.setBatchSize(100);
		fileProvider.setDataConsumer(entityConsumer);
		fileProvider.start();
		
		while(true){
			Thread.sleep(100);
		}
	}

}
