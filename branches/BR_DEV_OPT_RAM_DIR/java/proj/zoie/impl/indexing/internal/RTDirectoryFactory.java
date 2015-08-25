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
  public static final int DEFAULT_MAX_FREELISTSIZE = 4096; // 64M
  public static final RTDirectoryFactory INSTANCE = new RTDirectoryFactory();

  private final ReferenceQueue<RTFile> _refq;
  private final HashMap<Reference<RTFile>,Allocator> _allocs;
  private Buffer _freeList = null;
  private int _freeListSize = 0;
  private int _maxFreeListSize = DEFAULT_MAX_FREELISTSIZE;

  public RTDirectoryFactory()
  {
    _refq = new ReferenceQueue<RTFile>();
    _allocs = new HashMap<Reference<RTFile>,Allocator>();
  }

  public void setMaxFreeListSize(int maxFreeListSize)
  {
    _maxFreeListSize = maxFreeListSize;
    
    synchronized(this)
    {
      while(_freeListSize > _maxFreeListSize)
      {
        _freeList = _freeList._next;
        _freeListSize--;
      }
    }
  }
  
  public int getMaxFreeListSize()
  {
    return _maxFreeListSize;
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
    
    while(true)
    {
      Reference<? extends RTFile> ref = _refq.poll();
      if(ref == null) break;
      
      Allocator alloc;
      synchronized(_allocs)
      {
        alloc = _allocs.remove(ref);
      }
      if(alloc != null)
      {
        alloc.release();
      }
    }
    
    synchronized(this)
    {
      if(_freeList != null)
      {
        newBlock = _freeList;
        _freeList = newBlock._next;
        newBlock._next = null;
        _freeListSize--;
        return newBlock;
      }
      _freeListSize = 0;
    }
    return new Buffer();
  }
    
  private void free(Buffer head, Buffer tail, int size)
  {
    if(head == null || tail == null) return;
    
    if(_freeListSize >= _maxFreeListSize) return;
    
    synchronized(this)
    {
      tail._next = _freeList;
      _freeList = head;
      _freeListSize += size;
      while(_freeListSize > _maxFreeListSize)
      {
        _freeList = _freeList._next;
        _freeListSize--;
      }
    }
  }
  
  private static class Buffer
  {
    public final byte[] _block = new byte[BLOCKSIZE];
    public Buffer _next = null;
  }
  
  public static class Allocator
  {
    private RTDirectoryFactory _factory;
    private Buffer _head = null;
    private Buffer _tail = null;
    private int _size = 0;
    
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
        _size++;
      }
      return buf._block;
    }
    
    public void release()
    {
      synchronized(this)
      {
        _factory.free(_head, _tail, _size);
        _head = null;
        _tail = null;
      }
    }
  }
}