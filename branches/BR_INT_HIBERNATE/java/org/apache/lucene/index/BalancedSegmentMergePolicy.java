package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;

public class BalancedSegmentMergePolicy extends ZoieMergePolicy {

	  public static final int DEFAULT_NUM_LARGE_SEGMENTS = 10;
	  
	  private boolean _partialExpunge = false;
	  private int _numLargeSegments = DEFAULT_NUM_LARGE_SEGMENTS;
	  
	 public void setPartialExpunge(boolean doPartialExpunge)
	  {
	    _partialExpunge = doPartialExpunge;
	  }
	  
	  public boolean getPartialExpunge()
	  {
	    return _partialExpunge;
	  }
	  
	  public void setNumLargeSegments(int numLargeSegments)
	  {
	    _numLargeSegments = numLargeSegments;
	  }
	  
	  public int getNumLargeSegments()
	  {
	    return _numLargeSegments;
	  }
	  
	  private boolean isOptimized(SegmentInfos infos, IndexWriter writer, int maxNumSegments, Set segmentsToOptimize) throws IOException {
	    final int numSegments = infos.size();
	    int numToOptimize = 0;
	    SegmentInfo optimizeInfo = null;
	    for(int i=0;i<numSegments && numToOptimize <= maxNumSegments;i++) {
	      final SegmentInfo info = infos.info(i);
	      if (segmentsToOptimize.contains(info)) {
	        numToOptimize++;
	        optimizeInfo = info;
	      }
	    }

	    return numToOptimize <= maxNumSegments &&
	      (numToOptimize != 1 || isOptimized(writer, optimizeInfo));
	  }
	  
	  /** Returns true if this single nfo is optimized (has no
	   *  pending norms or deletes, is in the same dir as the
	   *  writer, and matches the current compound file setting */
	  private boolean isOptimized(IndexWriter writer, SegmentInfo info)
	    throws IOException {
	    return !info.hasDeletions() &&
	      !info.hasSeparateNorms() &&
	      info.dir == writer.getDirectory() &&
	      info.getUseCompoundFile() == getUseCompoundFile();
	  }

	  /** Returns the merges necessary to optimize the index.
	   *  This merge policy defines "optimized" to mean only one
	   *  segment in the index, where that segment has no
	   *  deletions pending nor separate norms, and it is in
	   *  compound file format if the current useCompoundFile
	   *  setting is true.  This method returns multiple merges
	   *  (mergeFactor at a time) so the {@link MergeScheduler}
	   *  in use may make use of concurrency. */
	  @Override
	  public MergeSpecification findMergesForOptimize(SegmentInfos infos, IndexWriter writer, int maxNumSegments, Set segmentsToOptimize) throws IOException {
	    
	    assert maxNumSegments > 0;

	    MergeSpecification spec = null;

	    if (!isOptimized(infos, writer, maxNumSegments, segmentsToOptimize))
	    {
	      // Find the newest (rightmost) segment that needs to
	      // be optimized (other segments may have been flushed
	      // since optimize started):
	      int last = infos.size();
	      while(last > 0)
	      {
	        final SegmentInfo info = infos.info(--last);
	        if (segmentsToOptimize.contains(info))
	        {
	          last++;
	          break;
	        }
	      }

	      if (last > 0)
	      {
	        if (maxNumSegments == 1)
	        {
	          // Since we must optimize down to 1 segment, the
	          // choice is simple:
	          boolean useCompoundFile = getUseCompoundFile();
	          if (last > 1 || !isOptimized(writer, infos.info(0)))
	          {
	            spec = new MergeSpecification();
	            spec.add(new OneMerge(infos.range(0, last), useCompoundFile));
	          }
	        }
	        else if (last > maxNumSegments)
	        {    
	          // find most balanced merges
	          spec = findBalancedMerges(infos, last, maxNumSegments, _partialExpunge);
	        }
	      }
	    }
	    return spec;
	  }
	  
	  private MergeSpecification findBalancedMerges(SegmentInfos infos, int infoLen, int maxNumSegments, boolean partialExpunge)
	    throws IOException
	  {
	    if (infoLen <= maxNumSegments) return null;
	    
	    MergeSpecification spec = new MergeSpecification();
	    boolean useCompoundFile = getUseCompoundFile();

	    // use Viterbi algorithm to find the best segmentation.
	    // we will try to minimize the size variance of resulting segments.
	    
	    final int maxMergeSegments = infoLen - maxNumSegments;
	    double[][] variance = createVarianceTable(infos, infoLen, maxMergeSegments);
	    
	    double[] sumVariance = new double[maxMergeSegments];
	    int[][] backLink = new int[maxNumSegments][maxMergeSegments];
	    
	    for(int i = (maxMergeSegments - 1); i >= 0; i--)
	    {
	      sumVariance[i] = variance[0][i];
	      backLink[0][i] = 0;
	    }
	    for(int i = 1; i < maxNumSegments; i++)
	    {
	      for(int j = (maxMergeSegments - 1); j >= 0; j--)
	      {
	        double minV = Double.MAX_VALUE;
	        int minK = 0;
	        for(int k = j; k >= 0; k--)
	        {
	          double v = sumVariance[k] + variance[i + k][j - k];
	          if(v < minV)
	          {
	            minV = v;
	            minK = k;
	          }
	        }
	        sumVariance[j] = minV;
	        backLink[i][j] = minK;
	      }
	    }
	    
	    // now, trace back the back links to find all merges,
	    // also find a candidate for partial expunge if requested
	    int mergeEnd = infoLen;
	    int prev = maxMergeSegments - 1;
	    int expungeCandidate = -1;
	    int maxDelCount = 0;
	    for(int i = maxNumSegments - 1; i >= 0; i--)
	    {
	      prev = backLink[i][prev];
	      int mergeStart = i + prev;
	      if((mergeEnd - mergeStart) > 1)
	      {
	        spec.add(new OneMerge(infos.range(mergeStart, mergeEnd), useCompoundFile));
	      }
	      else
	      {
	        if(partialExpunge)
	        {
	          SegmentInfo info = infos.info(mergeStart);
	          int delCount = info.getDelCount();
	          if(delCount > maxDelCount)
	          {
	            expungeCandidate = mergeStart;
	            maxDelCount = delCount;
	          }
	        }
	      }
	      mergeEnd = mergeStart;
	    }
	    
	    if(partialExpunge && maxDelCount > 0)
	    {
	      // expunge deletes
	      spec.add(new OneMerge(infos.range(expungeCandidate, expungeCandidate + 1), useCompoundFile));
	    }
	    
	    return spec;
	  }
	  
	  private double[][] createVarianceTable(SegmentInfos infos, int last, int maxMergeSegments) throws IOException
	  {
	    double[][] variance = new double[last][maxMergeSegments];
	    int maxNumSegments = last - maxMergeSegments;
	    
	    // compute the optimal segment size
	    long optSize = 0;
	    long[] sizeArr = new long[last];
	    for(int i = 0; i < sizeArr.length; i++)
	    {
	      sizeArr[i] = size(infos.info(i));
	      optSize += sizeArr[i];
	    }
	    optSize = (optSize / maxNumSegments);
	    
	    for(int i = 0; i < last; i++)
	    {
	      long size = 0;
	      for(int j = 0; j < maxMergeSegments; j++)
	      {
	        if((i + j) < last)
	        {
	          size += sizeArr[i + j];
	          double residual = ((double)size/(double)optSize) - 1.0d;
	          variance[i][j] = residual * residual;
	        }
	        else
	        {
	          variance[i][j] = Double.NaN;
	        }
	      }
	    }
	    return variance;
	  }
	  
	  /**
	   * Finds merges necessary to expunge all deletes from the
	   * index. The number of large segments will stay the same.
	   */ 
	  @Override
	  public MergeSpecification findMergesToExpungeDeletes(SegmentInfos infos, IndexWriter writer)
	    throws CorruptIndexException, IOException
	  {
	    final int numSegs = infos.size();
	    final int numLargeSegs = (numSegs < _numLargeSegments ? numSegs : _numLargeSegments);
	    MergeSpecification spec = null;
	    
	    if(numLargeSegs < numSegs)
	    {
	      SegmentInfos smallSegments = infos.range(numLargeSegs, numSegs);
	      spec = super.findMergesToExpungeDeletes(smallSegments, writer);
	    }
	    
	    if(spec == null) spec = new MergeSpecification();
	    for(int i = 0; i < numLargeSegs; i++)
	    {
	      SegmentInfo info = infos.info(i);
	      if(info.hasDeletions())
	      {
	        spec.add(new OneMerge(infos.range(i, i + 1), getUseCompoundFile()));        
	      }
	    }
	    return spec;
	  }
	  
	  /** Checks if any merges are now necessary and returns a
	   *  {@link MergePolicy.MergeSpecification} if so.
	   *  This merge policy try to maintain {@link
	   *  #setNumLargeSegments} of large segments in similar sizes.
	   *  {@link LogByteSizeMergePolicy} to small segments.
	   *  Small segments are merged and promoted to a large segment
	   *  when the total size reaches the average size of large segments.
	   */
	  @Override
	  public MergeSpecification findMerges(SegmentInfos infos, IndexWriter writer) throws IOException
	  {
	    final int numSegs = infos.size();
	    
	    if(numSegs == 0) return null;
	    
	    final int numLargeSegs = (numSegs < _numLargeSegments ? numSegs : _numLargeSegments);
	    long totalLargeSegSize = 0;
	    long totalSmallSegSize = 0;
	    SegmentInfo info;
	    
	    // compute the average size of large segments and 
	    // compare it with the total size of small segments.
	    for(int i = 0; i < numLargeSegs; i++)
	    {
	      info = infos.info(i);
	      totalLargeSegSize += size(info);
	    }
	    for(int i = numLargeSegs; i < numSegs; i++)
	    {
	      info = infos.info(i);
	      totalSmallSegSize += size(info);
	    }
	    
	    // if the total size of small segments exceeds the average size of large segments,
	    // promote the small segments to a large segment and do balanced merge,
	    // otherwise apply regular log merge policy to small segments.
	    if(numLargeSegs >= numSegs || (totalLargeSegSize / numLargeSegs) <= totalSmallSegSize)
	    {
	      return findBalancedMerges(infos, numSegs, numLargeSegs, _partialExpunge);
	    }
	    else
	    {
	      SegmentInfos smallSegments = infos.range(numLargeSegs, numSegs);
	      MergeSpecification spec = super.findMerges(smallSegments, writer);
	      
	      if(_partialExpunge)
	      {
	        OneMerge expunge  = findOneSegmentToExpunge(infos, numLargeSegs);
	        if(expunge != null)
	        {
	          if(spec == null) spec = new MergeSpecification();
	          spec.add(expunge);
	        }
	      }
	      return spec;
	    }      
	  }
	  
	  private OneMerge findOneSegmentToExpunge(SegmentInfos infos, int maxNumSegments) throws IOException
	  {
	    int expungeCandidate = -1;
	    int maxDelCount = 0;
	    
	    for(int i = maxNumSegments - 1; i >= 0; i--)
	    {
	      SegmentInfo info = infos.info(i);
	      int delCount = info.getDelCount();
	      if(delCount > maxDelCount)
	      {
	        expungeCandidate = i;
	        maxDelCount = delCount;
	      }
	    }
	    if(maxDelCount > 0)
	    {
	      return new OneMerge(infos.range(expungeCandidate, expungeCandidate + 1), getUseCompoundFile());
	    }
	    return null;
	  }
}
