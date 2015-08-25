package proj.zoie.test.mock;

import java.util.Collection;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;

public class MockDataLoader<V> implements DataConsumer<V> {
	private static final Logger log = Logger.getLogger(MockDataLoader.class);
	
	private long _delay;
	private int _count;
	private V _lastConsumed;
	
	public MockDataLoader()
	{
		_delay=100L;
		_count=0;
		_lastConsumed=null;
	}
	
	public V getLastConsumed()
	{
		return _lastConsumed;
	}
	
	public void setDelay(long delay)
	{
		_delay=delay;
	}
	
	public long getDelay()
	{
		return _delay;
	}
	
	public int getCount()
	{
		return _count;
	}
	
	public void consume(Collection<DataEvent<V>> data) throws ZoieException {
		
		if (data!=null && data.size()>0)
		{
			for (DataEvent<V> event : data)
			{
				_lastConsumed=event.getData();
			}
			_count+=data.size();
		}
	}
}
