/**
 * 
 */
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;

/**
 * @author ymatsuda
 *
 */
public class ZoieMergePolicy extends LogByteSizeMergePolicy
{
  
  public ZoieMergePolicy()
  {
    super();
  }
  
  protected long size(SegmentInfo info) throws IOException
  {
    long byteSize = info.sizeInBytes();
    float delRatio = (info.docCount <= 0 ? 0.0f : ((float)info.getDelCount() / (float)info.docCount));
    return (info.docCount <= 0 ?  byteSize : (long)((float)byteSize * (1.0f - delRatio)));
  }
  
 
}
