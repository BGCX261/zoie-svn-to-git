package proj.zoie.impl.indexing;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataProvider;
import proj.zoie.api.ZoieException;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.mbean.DataProviderAdminMBean;

public abstract class StreamDataProvider<V> implements DataProvider<V>,DataProviderAdminMBean{
	private static final Logger log = Logger.getLogger(StreamDataProvider.class);
	
	private int _batchSize;
	private DataConsumer<V> _consumer;
	private DataThread<V> _thread;
	
	public StreamDataProvider()
	{
		_batchSize=1;
		_consumer=null;
	}
	
	public void setDataConsumer(DataConsumer<V> consumer)
	{
	  _consumer=consumer;	
	}

	public abstract DataEvent<V> next();
	
	protected abstract void reset();
	
	public int getBatchSize() {
		return _batchSize;
	}

	public void pause() {
		if (_thread != null)
		{
			_thread.pauseDataFeed();
		}
	}

	public void resume() {
		if (_thread != null)
		{
			_thread.resumeDataFeed();
		}
	}

	public void setBatchSize(int batchSize) {
		_batchSize=Math.max(1, batchSize);
	}
	
	public void stop()
	{
		if (_thread!=null && _thread.isAlive())
		{
			_thread.terminate();
			try {
				_thread.join();
			} catch (InterruptedException e) {
				log.warn("stopping interrupted");
			}
		}
	}

	public void start() {
		if (_thread==null || !_thread.isAlive())
		{
			reset();
			_thread = new DataThread<V>(this);
			_thread.start();
		}
	}
	
	public void syncWthVersion(long timeInMillis, long version) throws ZoieException
	{
	  _thread.syncWthVersion(timeInMillis, version);
	}
	
	private static final class DataThread<V> extends Thread
	{
	    private Collection<DataEvent<V>> _batch;
		private long _currentVersion;
		private final StreamDataProvider<V> _dataProvider;
		private boolean _paused;
		private boolean _stop;
		
		DataThread(StreamDataProvider<V> dataProvider)
		{
			super("Stream DataThread");
			setDaemon(false);
			_dataProvider = dataProvider;
			_currentVersion = 0L;
			_paused = false;
			_stop = false;
			_batch = new LinkedList<DataEvent<V>>();
		}
		
		void terminate()
		{
			synchronized(this)
			{
	            _stop = true;
			   this.notifyAll();
			}
		}
		
		void pauseDataFeed()
		{
		    synchronized(this)
		    {
		        _paused = true;
		    }
		}
		
		void resumeDataFeed()
		{
			synchronized(this)
			{
	            _paused = false;
				this.notifyAll();
			}
		}
		
		private void flush()
	    {
	    	// FLUSH
		    Collection<DataEvent<V>> tmp;
		    tmp = _batch;
            _batch = new LinkedList<DataEvent<V>>();

		    try
	        {
		      if(_dataProvider._consumer!=null)
		      {
		    	  _dataProvider._consumer.consume(tmp);
		      }
	        }
	        catch (ZoieException e)
	        {
	          log.error(e.getMessage(), e);
	        }
	    }
		
		public long getCurrentVersion()
		{
			synchronized(this)
			{
		      return _currentVersion;
			}
		}
		
		public void syncWthVersion(long timeInMillis, long version) throws ZoieException
		{
		  long now = System.currentTimeMillis();
		  long due = now + timeInMillis;
		  synchronized(this)
		  {
		    while(_currentVersion < version)
		    {
		      if(now > due)
              {
                throw new ZoieException("sync timed out");
              }
		      try
		      {
		        this.wait(due - now);
		      }
		      catch(InterruptedException e)
		      {
	              log.warn(e.getMessage(), e);
		      }
		      now = System.currentTimeMillis();
		    }
		  }
		}

		public void run()
		{
			while (!_stop)
			{
                synchronized(this)
                {
				    while(_paused && !_stop)
				    {
				        try {
							this.wait();
					    } catch (InterruptedException e) {
					        continue;
					    }
				    }
                }
				if (!_stop)
				{
					DataEvent<V> data = _dataProvider.next();
					if (data!=null)
					{
					  synchronized(this)
					  {
						_batch.add(data);
						if (_batch.size()>=_dataProvider._batchSize)
						{
							flush();
						}
						_currentVersion=Math.max(_currentVersion, data.getVersion());
						this.notifyAll();
					  }
					}
					else
					{
					  synchronized(this)
					  {
						flush();
						_stop=true;
						return;
					  }
					}
				}
			}
		}
	}
}
