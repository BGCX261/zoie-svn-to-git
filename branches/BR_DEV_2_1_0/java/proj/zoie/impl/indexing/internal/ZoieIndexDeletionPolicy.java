/**
 * 
 */
package proj.zoie.impl.indexing.internal;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.FSDirectory;

/**
 * @author ymatsuda
 *
 */
public class ZoieIndexDeletionPolicy implements IndexDeletionPolicy
{
  private IndexCommit _lastCommit;
  private HashMap<String,Snapshot> _currentSnapshots = new HashMap<String,Snapshot>();
  
  public ZoieIndexDeletionPolicy()
  {
    _lastCommit = null;
  }

  public void onInit(List commits) throws IOException
  {
    processCommits(commits);
  }

  public void onCommit(List commits) throws IOException
  {
    processCommits(commits);
  }
  
  private synchronized void processCommits(List commits)
  {
    int size = commits.size();
    if(size == 0) return;
    
    IndexCommit indexCommit = null;
    for(Object commit : commits)
    {
      indexCommit = (IndexCommit)commit;
      if(--size > 0 && !_currentSnapshots.containsKey(indexCommit.getSegmentsFileName()))
      {
        indexCommit.delete();
      }
    }
    _lastCommit = indexCommit;
  }
  
  public synchronized Snapshot getSnapshot()
  {
    if(_lastCommit == null) return null; // no commit yet
    
    Snapshot snapshot;
    synchronized(_currentSnapshots)
    {
      String name = _lastCommit.getSegmentsFileName();
      snapshot = _currentSnapshots.get(name);
      if(snapshot == null)
      {
        snapshot = new Snapshot(_lastCommit);
        _currentSnapshots.put(name, snapshot);
      }
      else
      {
        snapshot.incRef();
      }
    }
    return snapshot;
  }

  public class Snapshot
  {
    private final IndexCommit _commit;
    private final File _dir;
    private int _refcount;
    
    public Snapshot(IndexCommit commit)
    {
      _commit = commit;
      FSDirectory fsdir = (FSDirectory)commit.getDirectory();
      _dir = fsdir.getFile();
      _refcount = 1;
    }
    
    public File getDirectory()
    {
      return _dir;
    }
    
    public Collection<String> getFileNames() throws IOException
    {
      return (Collection<String>)_commit.getFileNames();
    }
    
    public void close()
    {
      decRef();
    }
    
    private synchronized void incRef()
    {
      _refcount++;
    }
    
    private synchronized void decRef()
    {
      if(--_refcount <= 0)
      {
        synchronized(_currentSnapshots)
        {
          _currentSnapshots.remove(_commit.getSegmentsFileName());
        }
      }      
    }
    
    public void finalize()
    {
      _refcount = 0;
      close();
    }
  }
}
