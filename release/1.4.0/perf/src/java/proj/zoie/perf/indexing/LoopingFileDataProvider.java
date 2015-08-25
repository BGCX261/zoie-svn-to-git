package proj.zoie.perf.indexing;

import java.io.File;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.FileDataProvider;

public class LoopingFileDataProvider extends FileDataProvider
{

  public LoopingFileDataProvider(File dir)
  {
    super(dir);
  }

  @Override
  public DataEvent<File> next() 
  {
    DataEvent<File> next = super.next();
    if(next == null)
    {
      reset();
      next = super.next();
    }
    return next;
  }
}
