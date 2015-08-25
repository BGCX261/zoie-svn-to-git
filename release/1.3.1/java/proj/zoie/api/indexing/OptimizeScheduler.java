package proj.zoie.api.indexing;

public abstract class OptimizeScheduler {
  abstract public int numSegsScheduledForOptmization(int currentNumSegs,long lastTimeOptmized);
}
