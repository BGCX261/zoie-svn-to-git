package proj.zoie.mbean;

import proj.zoie.impl.indexing.StreamDataProvider;

public class DataProviderAdmin implements DataProviderAdminMBean {
    private final StreamDataProvider<?> _dataProvider;
    
    public DataProviderAdmin(StreamDataProvider<?> dataProvider)
    {
    	_dataProvider=dataProvider;
    }
    
	public int getBatchSize() {
		return _dataProvider.getBatchSize();
	}
	
	public long getEventCount() {
	  return _dataProvider.getEventCount();
	}
	
	public long getEventsPerMinute() {
	  return _dataProvider.getEventsPerMinute();
	}

    public long getMaxEventsPerMinute() {
      return _dataProvider.getMaxEventsPerMinute();
    }
    
    public void setMaxEventsPerMinute(long maxEventsPerMinute) {
      _dataProvider.setMaxEventsPerMinute(maxEventsPerMinute);
    }
    
    public String getStatus() {
      return _dataProvider.getStatus();
    }

	public void pause() {
		_dataProvider.pause();
	}

	public void resume() {
		_dataProvider.resume();
	}

	public void setBatchSize(int batchSize) {
		_dataProvider.setBatchSize(batchSize);
	}

	public void start() {
		_dataProvider.start();
	}

	public void stop() {
		_dataProvider.stop();
	}

}
