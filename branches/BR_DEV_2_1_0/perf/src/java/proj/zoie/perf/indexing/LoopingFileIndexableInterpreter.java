package proj.zoie.perf.indexing;

import java.io.File;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.impl.indexing.FileIndexableInterpreter;

public class LoopingFileIndexableInterpreter extends FileIndexableInterpreter
{
  protected static int maxUID;
  
  public LoopingFileIndexableInterpreter(int max)
  {
    maxUID = max;
  }
  
  @Override
  public ZoieIndexable convertAndInterpret(File src) {
	  id %= maxUID;
	    return super.convertAndInterpret(src);
  }
}
