package proj.zoie.impl.indexing;

import java.io.Serializable;

/**
 * <b>Experimental API</b>
 * @author jwang
 *
 */
public interface IndexingEventListener {
	public static class IndexingEvent implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
	}
	
	void handleIndexingEvent(IndexingEvent evt);
}
