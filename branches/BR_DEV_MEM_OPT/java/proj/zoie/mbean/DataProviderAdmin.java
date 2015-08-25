package proj.zoie.mbean;

import proj.zoie.impl.indexing.StreamDataProvider;

public class DataProviderAdmin implements DataProviderAdminMBean {
    private final StreamDataProvider _dataProvider;
    
    public DataProviderAdmin(StreamDataProvider dataProvider)
    {
    	_dataProvider=dataProvider;
    }
    
	public int getBatchSize() {
		return _dataProvider.getBatchSize();
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
