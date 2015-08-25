package proj.zoie.api.indexing;

public abstract class OptimizeScheduler {
  public enum OptimizeType
  {
    FULL, PARTIAL, NONE
  };
  
  abstract public OptimizeType getScheduledOptimizeType();
  abstract public void finished();
}
