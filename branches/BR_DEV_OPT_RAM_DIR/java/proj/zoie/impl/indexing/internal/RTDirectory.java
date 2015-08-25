/**
 * 
 */
package proj.zoie.impl.indexing.internal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

/**
 * @author ymatsuda
 *
 */
public class RTDirectory extends Directory
{
  private final RTDirectoryFactory _factory;
  private HashMap<String,RTFile> _dir = new HashMap<String,RTFile>();
  
  public RTDirectory(RTDirectoryFactory factory)
  {
    _factory = factory;
    setLockFactory(new SingleInstanceLockFactory());
  }

  public final String[] list()
  {
    synchronized(_dir)
    {
      Set<String> fileNames = _dir.keySet();
      return fileNames.toArray(new String[fileNames.size()]);
    }
  }
  
  private RTFile getFile(String name)
  {
    synchronized(_dir)
    {
      return _dir.get(name);
    }
  }
  
  private RTFile getExistingFile(String name) throws FileNotFoundException
  {
    RTFile file = getFile(name);
    if(file == null) throw new FileNotFoundException(name);
    return file;
  }

  public final boolean fileExists(String name)
  {
    return (getFile(name) != null);
  }
  
  public final long fileModified(String name) throws IOException
  {
    RTFile file = getExistingFile(name);
    return file._lastModified;
  }
  
  public void touchFile(String name) throws IOException
  {
    RTFile file = getExistingFile(name);
    file._lastModified = System.currentTimeMillis();
  }
  
  public final long fileLength(String name) throws IOException
  {
    RTFile file = getExistingFile(name);
    return file._fileLength;
  }
  
  public void deleteFile(String name) throws IOException
  {
    synchronized(_dir)
    {
      RTFile file = _dir.remove(name);
      file.delete();
    }
  }

  public IndexOutput createOutput(String name) throws IOException
  {
    RTFile file;
    synchronized(_dir)
    {
      file = getFile(name);
      if(file != null) file.delete();
      
      file = _factory.createFile();
      _dir.put(name, file);
    }
    return file.createOutput();
  }

  public IndexInput openInput(String name) throws IOException
  {
    RTFile file = getFile(name);
    return file.createInput();
  }

  public void close()
  {
    if(_dir != null)
    {
      synchronized(_dir)
      {
        for(String fileName : _dir.keySet())
        {
          try
          {
            deleteFile(fileName);
          }
          catch(IOException e)
          {
          }
        }
      }
      _dir = null;
    }
  }
  
  @Override
  public void renameFile(String fromName, String toName) throws IOException
  {
    synchronized(_dir)
    {
      RTFile fromFile = _dir.get(fromName);
      if(fromFile == null) throw new FileNotFoundException(fromName);
      
      RTFile toFile = _dir.get(toName);
      if(toFile != null) toFile.delete();
      
      _dir.remove(fromName);
      _dir.put(toName, fromFile);
    }
  }
  
  public long getSize()
  {
    long size = 0;
    synchronized(_dir)
    {
      for(RTFile file : _dir.values())
      {
        size += file._size;
      }
    }
    return size;
  }
}
