package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public final class ReaderDirectory<R extends IndexReader>
{
  public static final class ReaderEntry<R extends IndexReader>
  {
    public final ZoieIndexReader undecorated;
    public final R decorated;
    
    public ReaderEntry(ZoieIndexReader undecorated,IndexReaderDecorator<R> decorator) throws IOException
    {
      this.undecorated = undecorated;
      this.decorated = decorator.decorate(undecorated);
    }
    
    @Override
    public String toString()
    {
      StringBuilder buf = new StringBuilder();
      buf.append("undecorated: ").append(undecorated);
      buf.append("\ndecorated: ").append(decorated);
      return buf.toString();
    }
  }
  
  private final IndexSignature _sig;
  private final TreeMap<String,ReaderEntry<R>> _directoryMap;
  
  public ReaderDirectory(IndexSignature sig)
  {
    _sig = sig;
    _directoryMap = new TreeMap<String,ReaderEntry<R>>();
  }
  
  public void addReaderEntry(String name, ReaderEntry<R> entry)
  {
    _directoryMap.put(name,entry);
  }
  
  public ReaderEntry<R> getReaderEntry(String segmentName)
  {
    return _directoryMap.get(segmentName);
  }
  
  public void setDeleteSet(IntSet delSet)
  {
    Collection<ReaderEntry<R>> entries = _directoryMap.values();
    for(ReaderEntry<R> entry : entries)
    {
      entry.undecorated.setDelSet(delSet);
    }
  }
  
  public int getNumDocs()
  {
    Collection<ReaderEntry<R>> entries = _directoryMap.values();
    int numDocs = 0;
    
    for(ReaderEntry<R> entry : entries)
    {
      numDocs += entry.undecorated.numDocs();
    }
    return numDocs;
  }
  
  public IndexSignature getSignature()
  {
    return _sig;
  }
  
  public Collection<R> getDecoratedReaders()
  {
    Collection<ReaderEntry<R>> entries = _directoryMap.values();
    ArrayList<R> readerList = new ArrayList<R>(entries.size());
    
    for (ReaderEntry<R> entry: entries)
    {
      readerList.add(entry.decorated);
    }
    return readerList;
  }
  
  public void dispose()
  {
    _directoryMap.clear();
  }
}
