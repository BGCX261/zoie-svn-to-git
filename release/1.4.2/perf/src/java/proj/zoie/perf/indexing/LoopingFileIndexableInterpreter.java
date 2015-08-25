package proj.zoie.perf.indexing;

import java.io.File;

import org.apache.log4j.Logger;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.impl.indexing.FileIndexableInterpreter;

public class LoopingFileIndexableInterpreter extends FileIndexableInterpreter
{
  private static final Logger log = Logger.getLogger(LoopingFileIndexableInterpreter.class);
  protected static int maxUID;
  
  public LoopingFileIndexableInterpreter(int max)
  {
    maxUID = max;
    log.info("constructor: " + max);
  }
  
  @Override
  public ZoieIndexable convertAndInterpret(File src) {
    id %= maxUID;
    return super.convertAndInterpret(src);
  }
}
