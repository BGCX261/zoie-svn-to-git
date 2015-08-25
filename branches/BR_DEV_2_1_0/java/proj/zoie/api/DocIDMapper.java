/**
 * 
 */
package proj.zoie.api;

/**
 * @author ymatsuda
 *
 */
public interface DocIDMapper
{
  /**
   * maps uid to a lucene docid
   * @param uid
   * @return -1 if uid is not found
   */
  int getDocID(long uid);
}
