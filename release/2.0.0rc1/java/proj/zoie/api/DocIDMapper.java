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
  public static final int NOT_FOUND = -1;
  /**
   * maps uid to a lucene docid
   * @param uid
   * @return NOT_FOUND if uid is not found
   */
  int getDocID(long uid);
}
