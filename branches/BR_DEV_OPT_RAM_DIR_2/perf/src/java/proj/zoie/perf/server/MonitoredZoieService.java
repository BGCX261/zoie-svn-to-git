package proj.zoie.perf.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class MonitoredZoieService<R extends IndexReader> implements ZoieSearchService
{
  protected static int NANOS_IN_MILLI = 1000000;
  protected List<Integer> _latencies = boundedList(Integer.class, 10000);
  protected List<Integer> _numHits = boundedList(Integer.class, 10000);
  protected Map<Long, Integer> _qps = new HashMap<Long, Integer>();
  protected final AtomicInteger _numSearches = new AtomicInteger(0);

  private static final Logger log = Logger.getLogger(MonitoredZoieService.class);
	
  private final IndexReaderFactory<R> _idxReaderFactory;
  
  public MonitoredZoieService(IndexReaderFactory<R> idxReaderFactory)
  {
	  _idxReaderFactory = idxReaderFactory;
  }
  
  public SearchResult search(SearchRequest req) throws ZoieException
  {
    long now = System.nanoTime();
    String queryString=req.getQuery();
	Analyzer analyzer=_idxReaderFactory.getAnalyzer();
	QueryParser qparser=new QueryParser("content",analyzer);
	
	SearchResult result=new SearchResult();
	
	List<R> readers=null;
	Searcher searcher = null;
	MultiReader multiReader=null;
	try
	{
		long start = System.nanoTime();
		Query q=null;
		if (queryString == null || queryString.length() ==0)
		{
			q = new MatchAllDocsQuery();
		}
		else
		{
			q = qparser.parse(queryString); 
		}
		readers=_idxReaderFactory.getIndexReaders();
		multiReader=new MultiReader(readers.toArray(new IndexReader[readers.size()]));
		searcher=new IndexSearcher(multiReader);
		TopDocs hits = searcher.search(q,10);
		result.setTotalDocs(multiReader.numDocs());
		result.setTotalHits(hits.totalHits);
		long end = System.nanoTime();
		result.setTime((end-start)/NANOS_IN_MILLI);
//	    countQuery((end / 1000 * NANOS_IN_MILLI));
//	    now = end - now;
//	    _latencies.add( (int)(now / NANOS_IN_MILLI));
//	    _numHits.add(hits.totalHits);
		log.info("search=[query=" + req.getQuery() + "]" + ", searchResult=[numSearchResults=" + result.getTotalHits() + ";numTotalDocs=" + result.getTotalDocs() + "]" + "in " + result.getTime() + "ms"); 
	    _numSearches.incrementAndGet();
	    return result;
	}
	catch(Exception e)
	{
		log.error(e.getMessage(),e);
		throw new ZoieException(e.getMessage(),e);
	}
	finally
	{
		if (searcher!=null)
		{
			try {
				searcher.close();
			} catch (IOException e) {
				log.error(e.getMessage(),e);
			}
		}
	}
  }
  
  protected void countQuery(long time)
  {
    if(!_qps.containsKey(time)) { _qps.put(time, 0); }
    _qps.put(time, _qps.get(time) + 1);
  }

  public int percentileLatency(int pct)
  {
    return percentile(_latencies, pct);
  }
  
  public int percentileHits(int pct)
  {
    return percentile(_numHits, pct);
  }
  
  public int percentileQps(int pct)
  {
    List<Integer> qps = new ArrayList<Integer>(_qps.size());
    qps.addAll(_qps.values());
    Collections.sort(qps);
    return percentile(qps, pct);
  }
  
  public int numSearches()
  {
    return _numSearches.get();
  }
  
  
  private int percentile(List<Integer> list, int pct)
  {
    Integer[] values = list.toArray(new Integer[list.size()]);
    Arrays.sort(values);
    return values[(pct/100)*values.length];
  }
  
  private static <T> List<T> boundedList(Class<T> t, final int maxSize)
  {
    return new LinkedList<T>()
    {
      @Override
      public boolean add(T t)
      {
        if(size() >= maxSize) removeFirst();
        return super.add(t);
      }
    };
  }

}
