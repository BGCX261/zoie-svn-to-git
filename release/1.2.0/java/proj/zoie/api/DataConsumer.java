package proj.zoie.api;

import java.util.Collection;

/**
 * interface for consuming a collection of data events
 * @author jwang
 *
 * @param <V>
 */
public interface DataConsumer<V> {
	
	/**
	 * Data event abstraction
	 * @author jwang
	 *
	 * @param <V>
	 */
	public static final class DataEvent<V>
	{
		private long _version;
		private V _data;
		
		public DataEvent(long version,V data)
		{
			_data=data;
			_version=version;
		}
		
		public long getVersion()
		{
			return _version;
		}
		
		public V getData()
		{
			return _data;
		}
	}
	
	/**
	 * consumption of a collection of data events.
	 * @param data
	 * @throws ZoieException
	 */
	void consume(Collection<DataEvent<V>> data) throws ZoieException;
}
