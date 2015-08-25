package proj.zoie.hibernate.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Store;

@Entity
public class TestEntity {
	  @Id
	  @GeneratedValue
	  private final int id; 

	  @Field(index = Index.TOKENIZED, store = Store.YES)
	  private final String content;
	  
	  @Field(index = Index.TOKENIZED, store = Store.YES)
	  private final String path;
	  
	  public TestEntity(int id,String content,String path){
		  this.id=id;
		  this.content=content;
		  this.path=path;
	  }

	public int getId() {
		return id;
	}

	public String getContent() {
		return content;
	}

	public String getPath() {
		return path;
	} 
}
