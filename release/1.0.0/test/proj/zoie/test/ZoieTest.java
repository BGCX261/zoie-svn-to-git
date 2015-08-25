package proj.zoie.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.test.data.TestData;
import proj.zoie.test.data.TestDataInterpreter;
import proj.zoie.test.mock.MockDataLoader;

public class ZoieTest extends TestCase {

	public ZoieTest() {
	}

	public ZoieTest(String name) {
		super(name);
	}
	
	private static File getIdxDir()
	{
		File tmpDir=new File(System.getProperty("java.io.tmpdir"));
		return new File(tmpDir,"test-idx");
	}
	
	private static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime)
	{
		ZoieSystem<IndexReader,String> idxSystem=new ZoieSystem<IndexReader, String>(idxDir,new TestDataInterpreter(),new IndexReaderDecorator<IndexReader>()
		{
			public IndexReader decorate(ZoieIndexReader indexReader)
					throws IOException {
				return indexReader;
			}},null,null,50,100,realtime);
		return idxSystem;
	}
	
	private static boolean deleteDirectory(File path) {
	    if( path.exists() ) {
	      File[] files = path.listFiles();
	      for(int i=0; i<files.length; i++) {
	         if(files[i].isDirectory()) {
	           deleteDirectory(files[i]);
	         }
	         else {
	           files[i].delete();
	         }
	      }
	    }
	    return( path.delete() );
	  }
	
	private static Searcher getSearcher(ZoieSystem<IndexReader,String> zoie) throws IOException
	{
		List<IndexReader> readers=zoie.getIndexReaders();
		MultiReader reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]));
		
		IndexSearcher searcher=new IndexSearcher(reader);
		return searcher;
	}
	
	public void testRealtime() throws ZoieException
	{
		File idxDir=getIdxDir();
		ZoieSystem<IndexReader,String> idxSystem=createZoie(idxDir,true);
		idxSystem.start();
		String query="zoie";
		QueryParser parser=new QueryParser("contents",idxSystem.getAnalyzer());
		Query q=null;
		try 
		{
			q=parser.parse(query);
		} catch (ParseException e) {
			throw new ZoieException(e.getMessage(),e);
		}
		MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
		memoryProvider.setDataConsumer(idxSystem);
		memoryProvider.start();
		try
		{
			int count=TestData.testdata.length;
			List<DataEvent<String>> list=new ArrayList<DataEvent<String>>(count);
			for (int i=0;i<count;++i)
			{
				list.add(new DataEvent<String>(i,TestData.testdata[i]));
			}
			memoryProvider.addEvents(list);
			memoryProvider.syncWthVersion(10000, count-1);
			
			// should be consumed by the idxing system
			Searcher searcher=null;
			try
			{
				searcher=getSearcher(idxSystem);
				
				Hits hits=searcher.search(q);
				
				assertEquals(count, hits.length());
			}
			finally
			{
				if (searcher!=null)
				{
					searcher.close();
					searcher=null;
				}
			}
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			for (int i=0;i<10;++i)
			{
				try
				{
					searcher=getSearcher(idxSystem);
					
					Hits hits=searcher.search(q);
					
					assertEquals(count, hits.length());
				}
				finally
				{
					if (searcher!=null)
					{
						searcher.close();
						searcher=null;
					}
				}	
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}	
			}
			
			try
			{
				searcher=getSearcher(idxSystem);
				
				Hits hits=searcher.search(q);
				
				assertEquals(count, hits.length());
			}
			finally
			{
				if (searcher!=null)
				{
					searcher.close();
					searcher=null;
				}
			}
		}
		catch(IOException ioe)
		{
			throw new ZoieException(ioe.getMessage());
		}
		finally
		{
			memoryProvider.stop();
			idxSystem.shutdown();
			deleteDirectory(idxDir);
		}	
	}
	
	public void testStreamDataProvider() throws ZoieException
	{
		MockDataLoader<Integer> consumer=new MockDataLoader<Integer>();
		MemoryStreamDataProvider<Integer> memoryProvider=new MemoryStreamDataProvider<Integer>();
		memoryProvider.setDataConsumer(consumer);
		memoryProvider.start();
		try
		{
			int count=10;
			List<DataEvent<Integer>> list=new ArrayList<DataEvent<Integer>>(count);
			for (int i=0;i<count;++i)
			{
				list.add(new DataEvent<Integer>(i,i));
			}
			memoryProvider.addEvents(list);
			
			memoryProvider.syncWthVersion(10000, count-1);
			int num=consumer.getCount();
			assertEquals(num, count);	
		}
		finally
		{
			memoryProvider.stop();
		}
	}
}
