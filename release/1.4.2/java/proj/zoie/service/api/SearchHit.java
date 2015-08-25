package proj.zoie.service.api;

import java.io.Serializable;
import java.util.Map;

public class SearchHit implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private float score;
	private Map<String,String[]> fields;
	
	public SearchHit()
	{
		
	}
	
	public float getScore() {
		return score;
	}
	
	public void setScore(float score) {
		this.score = score;
	}
	
	public Map<String, String[]> getFields() {
		return fields;
	}
	
	public void setFields(Map<String, String[]> fields) {
		this.fields = fields;
	}
}
