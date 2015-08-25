package proj.zoie.api;

import java.util.Collection;
import java.util.Comparator;

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
		static Comparator<DataEvent<?>> VERSION_COMPARATOR = new EventVersionComparator();
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
	    
		static public Comparator<DataEvent<?>> getComparator()
		{
		  return VERSION_COMPARATOR;
		}
		
	    static public class EventVersionComparator implements Comparator<DataEvent<?>>
	    {
	      public int compare(DataEvent<?> o1, DataEvent<?> o2)
          {
            if(o1._version < o2._version) return -1;
            else if(o1._version > o2._version) return 1;
            else return 0; 
          }
	      public boolean equals(DataEvent<?> o1, DataEvent<?> o2)
	      {
	        return (o1._version == o2._version);
	      }
	    }
	}
	
	/**
	 * Consumption of a collection of data events.
	 * Note that this method may have a side effect. That is it may empty the Collection passed in after execution.
	 * It is good practice if the data collection along with its contents passed to consume(data) never changed by client code later.
	 * We also strengthen the contract on this method that the events in data are sorted according to their version numbers and
	 * if consume is invoked on collection1 before on collection2, then all the max version number for events in collection1
	 * must be smaller than the min version number for events in collection2.
	 * @param data
	 * @throws ZoieException
	 */
	void consume(Collection<DataEvent<V>> data) throws ZoieException;
}
