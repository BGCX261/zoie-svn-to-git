package proj.zoie.impl.indexing.internal;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public interface RamDirectoryFactory {
  Directory newRamDirectory();
  
  public final static RamDirectoryFactory DEFAULT = new DefaultRamDirectoryFactory();
  
  public static class DefaultRamDirectoryFactory implements RamDirectoryFactory
  {

	public Directory newRamDirectory() {
		return new RAMDirectory();
	}
  }
}
