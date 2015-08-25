package proj.zoie.service.api;

import java.io.Serializable;

public class SearchRequest implements Serializable 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String query;
	
	public SearchRequest()
	{
		query=null;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
}
