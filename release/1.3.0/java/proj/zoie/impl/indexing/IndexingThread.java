package proj.zoie.impl.indexing;

import org.apache.log4j.Logger;

public class IndexingThread extends Thread
{
  private static final Logger log = Logger.getLogger(IndexingThread.class);
  private static final Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler()
  {
    public void uncaughtException(Thread thread, Throwable t)
    {
      log.error(thread.getName() + " is abruptly terminated", t);
    }
  };
  
  public IndexingThread(String name)
  {
    super(name);
    this.setUncaughtExceptionHandler(exceptionHandler);
  }
}
