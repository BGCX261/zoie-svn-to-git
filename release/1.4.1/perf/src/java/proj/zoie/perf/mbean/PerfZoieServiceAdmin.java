package proj.zoie.perf.mbean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.perf.server.MonitoredZoieService;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;

public class PerfZoieServiceAdmin implements PerfZoieServiceMBean
{
  protected static int numThreads = 4;
  protected int _waitTimeMillis = 0;
  protected ExecutorService _threadPool = Executors.newFixedThreadPool(numThreads + 1);
  protected boolean _perfRunStarted = false;
  
  protected ZoieSystem _zoieSystem;
  protected MonitoredZoieService _svc;
  protected Thread _perfThread;
  
  public void setWaitTimeMillis(int waitTimeMillis)
  {
    _waitTimeMillis = waitTimeMillis;
  }
  
  public int getWaitTimeMillis()
  {
    return _waitTimeMillis;
  }
  
  public void setMonitoredZoieService(MonitoredZoieService svc)
  {
    _svc = svc;
  }
  
  public MonitoredZoieService getMonitoredZoieService()
  {
    return _svc;
  }
  
  public void setZoieSystem(ZoieSystem system)
  {
    _zoieSystem = system;
  }
  
  public ZoieSystem getZoieSystem()
  {
    return _zoieSystem;
  }

  public void startPerfRun()
  {
    if(!_perfRunStarted)
    {
      _perfRunStarted = true;
      _perfThread = new Thread(new QueryDriverRunnable());
      _perfThread.start();
    }
  }
  
  public void endPerfRun()
  {
    _perfRunStarted = false;
  }

  public int percentileLatency(int pct)
  {
    return _svc.percentileLatency(pct);
  }

  public int percentileQPS(int pct)
  {
    return _svc.percentileQps(pct);
  }
  
  public int percentileHits(int pct)
  {
    return _svc.percentileHits(pct);
  }

  protected class QueryDriverRunnable implements Runnable
  {
    public void run()
    {
      List<String> queryTerms = new ArrayList<String>();
      IndexReader diskReader = null;
      TermEnum terms = null;
      try
      {
        diskReader = _zoieSystem.getDiskIndexReader();
        int numDocs = diskReader.numDocs();
        terms = diskReader.terms();
        while(terms.next() && queryTerms.size() < 10000)
        {
          Term term = terms.term();
          int docFreq = diskReader.docFreq(term);
          if(docFreq * 100 >= numDocs) queryTerms.add(term.text());
        }
      }
      catch (IOException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      finally
      {
        try
        {
          if(terms != null)
            terms.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
      while(_perfRunStarted)
      {
        int i = (int) (queryTerms.size() * Math.random());
        String q = queryTerms.get(i);
        final SearchRequest req = new SearchRequest();
        req.setQuery(q);
        _threadPool.submit(new Callable<SearchResult>()
        {
          public SearchResult call() throws Exception
          {
            Thread.sleep(_waitTimeMillis);
            return _svc.search(req);
          }
        });
      }
    }
  }

  public int getNumSearches()
  {
    return _svc.numSearches();
  }

}
