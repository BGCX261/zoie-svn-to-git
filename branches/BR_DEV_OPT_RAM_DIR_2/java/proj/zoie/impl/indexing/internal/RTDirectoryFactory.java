/**
 * 
 */
package proj.zoie.impl.indexing.internal;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;

import org.apache.lucene.store.Directory;

/**
 * @author ymatsuda
 *
 */
public class RTDirectoryFactory implements RamDirectoryFactory
{
  public static final int SHIFT = 14;
  public static final int BLOCKSIZE = (1 << SHIFT); // 16K
  public static final int MASK = (BLOCKSIZE - 1);
  public static final RTDirectoryFactory INSTANCE = new RTDirectoryFactory();

  private final ReferenceQueue<RTFile> _refq;
  private final HashMap<Reference<RTFile>,Allocator> _allocs;
  private Buffer _freeList = null;

  public RTDirectoryFactory()
  {
    _refq = new ReferenceQueue<RTFile>();
    _allocs = new HashMap<Reference<RTFile>,Allocator>();
  }

  public Directory newRamDirectory()
  {
    return new RTDirectory(this);
  }
  
  public RTFile createFile()
  {
    Allocator allocator = new Allocator(this);
    RTFile file = new RTFile(allocator);
    Reference<RTFile> ref = new WeakReference<RTFile>(file, _refq);
    synchronized(_allocs)
    {
      _allocs.put(ref, allocator);
    }
    return file;
  }
  
  private Buffer getBuffer()
  {
    Buffer newBlock = null;

    synchronized(_refq)
    {
      while(true)
      {
        Reference<? extends RTFile> ref = _refq.poll();
        if(ref == null) break;
        
        Allocator alloc = _allocs.remove(ref);
        if(alloc != null)
        {
          alloc.release();
        }
      }
    }
    
    synchronized(this)
    {
      if(_freeList != null)
      {
        newBlock = _freeList;
        _freeList = newBlock._next;
        newBlock._next = null;
        return newBlock;
      }
    }
    return new Buffer();
  }
    
  private void free(Buffer head, Buffer tail)
  {
    if(head == null || tail == null) return;
    
    synchronized(this)
    {
      tail._next = _freeList;
      _freeList = head;
    }
  }
  
  private static class Buffer
  {
    public final byte[] _block = new byte[BLOCKSIZE];
    public Buffer _next = null;
  }
  
  public static class Allocator
  {
    public RTDirectoryFactory _factory;
    public Buffer _head = null;
    public Buffer _tail = null;
    
    public Allocator(RTDirectoryFactory factory)
    {
      _factory = factory;
    }
    
    public byte[] alloc()
    {
      Buffer buf = _factory.getBuffer();
      synchronized(this)
      {
        buf._next = _head;
        _head = buf;
        if(_tail == null) _tail = buf;
      }
      return buf._block;
    }
    
    public void release()
    {
      synchronized(this)
      {
        _factory.free(_head, _tail);
        _head = null;
        _tail = null;
      }
    }
  }
}