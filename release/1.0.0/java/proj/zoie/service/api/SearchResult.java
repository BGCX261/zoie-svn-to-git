package proj.zoie.service.api;

import java.io.Serializable;
import java.util.Map;

public class SearchResult implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int totalHits;
	private int totalDocs;
	private long time;
	
	public SearchResult()
	{
		
	}
	
	private SearchHit[] hits;

	public int getTotalHits() {
		return totalHits;
	}

	public void setTotalHits(int totalHits) {
		this.totalHits = totalHits;
	}

	public int getTotalDocs() {
		return totalDocs;
	}

	public void setTotalDocs(int totalDocs) {
		this.totalDocs = totalDocs;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public SearchHit[] getHits() {
		return hits;
	}

	public void setHits(SearchHit[] hits) {
		this.hits = hits;
	}
	
}
