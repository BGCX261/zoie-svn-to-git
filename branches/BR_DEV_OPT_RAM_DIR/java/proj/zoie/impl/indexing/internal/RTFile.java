/**
 * 
 */
package proj.zoie.impl.indexing.internal;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import proj.zoie.impl.indexing.internal.RTDirectoryFactory.Allocator;

/**
 * @author ymatsuda
 *
 */
public class RTFile
{
  private static final int BLOCKSIZE = RTDirectoryFactory.BLOCKSIZE;
  private static final int MASK = RTDirectoryFactory.MASK;
  private static final int SHIFT = RTDirectoryFactory.SHIFT;
  
  private ArrayList<byte[]> _blocks = new ArrayList<byte[]>();
  
  public volatile long _fileLength = 0;
  public volatile long _lastModified = System.currentTimeMillis();
  public volatile long _size = 0;
  private Allocator _allocator;
  
  public RTFile(Allocator allocator)
  {
    _allocator = allocator;
  }
  
  public void delete()
  {
    _blocks = null;
    _allocator.release();
    _allocator = null;
  }
  
  public IndexInput createInput()
  {
    return new RTIndexInput();
  }
  
  public IndexOutput createOutput()
  {
    return new RTIndexOutput();
  }
  
  private byte[] addBlock()
  {
    byte[] block = _allocator.alloc();
      
    _blocks.add(block);
    _size += BLOCKSIZE;

    return block;
  }
  
  public class RTIndexInput extends IndexInput implements Cloneable
  {
    private byte[] _block;
    private int _blockId;
    
    private long _bufStart;
    private int _bufPos;
    private int _bufLeft;
    
    private boolean _posUndefined = false;

    public RTIndexInput()
    {
      _blockId = -1;
      _block = null;
      _bufStart = 0;
      _bufPos = 0;
      _bufLeft = 0;
    }
    
    public long length()
    {
      return _fileLength;
    }

    public byte readByte() throws IOException
    {
      if(_bufLeft == 0)
      {
        if(!getBlock(_blockId + 1)) throw new IOException("Undefined position");
      }
      _bufLeft--;
      return _block[_bufPos++];
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException
    {
      while(len > 0) 
      {
        if(_bufLeft == 0)
        {
          if(!getBlock(_blockId + 1)) throw new IOException("Undefined position");
        }

        int amount = (len < _bufLeft ? len : _bufLeft);
        System.arraycopy(_block, _bufPos, b, offset, amount);
        offset += amount;
        len -= amount;
        _bufPos += amount;
        _bufLeft -= amount;
      }
    }

    public long getFilePointer()
    {
      return (_bufStart + _bufPos);
    }

    public void seek(long pos) throws IOException
    {
      if(pos < 0 || pos >= _fileLength)
      {
        setPositionUndefined();
      }
      
      int blockId = (int)(pos >> SHIFT);
      int blockPos = ((int)pos & MASK);
      if(blockId == _blockId)
      {
        _bufLeft -= (blockPos + _bufPos);
        _bufPos = blockPos;
      }
      else
      {
        _posUndefined = false;
        
        getBlock(blockId);
        _bufLeft -= blockPos;
        _bufPos = blockPos;
      }
    }
    
    private final boolean getBlock(int blockId) throws IOException
    {
      if (_posUndefined || blockId >= _blocks.size())
      {
        setPositionUndefined();
        return false;
      }

      _block = _blocks.get(blockId);
      _blockId = blockId;
      _bufPos = 0;
      _bufStart = (long)BLOCKSIZE * (long)_blockId;
      long buflen = (int)(_fileLength - _bufStart);
      _bufLeft = buflen > BLOCKSIZE ? BLOCKSIZE : (int)buflen;
      return true;
    }
    
    private final void setPositionUndefined()
    {
      _posUndefined = true;
      _blockId = -1;
      _block = null;
      _bufPos = 0;
      _bufLeft = 0;      
    }
    
    @Override
    public void close() throws IOException
    {
    }
  }
  
  public class RTIndexOutput extends IndexOutput implements Cloneable
  {
    private byte[] _block;
    private int _blockId;
    
    private int _bufPos;
    private long _bufStart;
    private boolean _modified;

    public RTIndexOutput()
    {
      _blockId = -1;
      _block = null;
      _bufStart = 0;
      _bufPos = 0;
      _modified = false;
    }
    
    public void close() throws IOException
    {
      flush();
    }
    
    public void writeTo(IndexOutput out) throws IOException
    {
      flush();
      final int lastBlockId = _blocks.size() - 1;
      for(int i = 0; i < lastBlockId; i++)
      {
        out.writeBytes(_blocks.get(i), BLOCKSIZE);
      }
      if(lastBlockId >= 0)
      {
        int length = ((int)_fileLength & MASK);
        out.writeBytes(_blocks.get(lastBlockId), length);
      }
    }

    public void seek(long pos) throws IOException
    {
      int blockId = (int)(pos >> SHIFT);
      if (blockId != _blockId)
      {
        getBlock(blockId);
      }
      _bufPos = ((int)pos & MASK);
    }

    public long length()
    {
      updateFileLength();
      return _fileLength;
    }

    public void writeByte(byte b) throws IOException
    {
      if(_bufPos == BLOCKSIZE)
      {
        getBlock(_blockId + 1);
      }
      _block[_bufPos++] = b;
      _modified = true;
    }

    public void writeBytes(byte[] b, int offset, int len) throws IOException
    {
      while (len > 0)
      {
        if(_bufPos == BLOCKSIZE)
        {
          getBlock(_blockId + 1);
        }

        int amount = BLOCKSIZE - _bufPos;
        if(amount > len) amount = len;
        System.arraycopy(b, offset, _block, _bufPos, amount);
        offset += amount;
        len -= amount;
        _bufPos += amount;
        _modified = true;
      }
    }

    public void flush() throws IOException
    {
      updateFileLength();
      updateLastModified();
    }

    public long getFilePointer()
    {
      return (_bufStart + _bufPos);
    }
    
    private final void updateFileLength()
    {
      long pos = _bufStart + _bufPos;
      if(pos > _fileLength) _fileLength = pos;
    }
    
    private final void updateLastModified()
    {
      if(_modified)
      {
        _lastModified = System.currentTimeMillis();
        _modified = false;
      }
    }
    
    private final boolean getBlock(int blockId) throws IOException
    {
      flush(); // updates length and last modified
      
      if (blockId == _blocks.size())
      {
        _block = addBlock();
      }
      else
      {
        try
        {
          _block = _blocks.get(blockId);
        }
        catch(RuntimeException e)
        {
          throw new IOException();
        }
      }
      _blockId = blockId;
      _bufPos = 0;
      _bufStart = (long)BLOCKSIZE * (long)blockId;
      return true;
    }
  }
}
