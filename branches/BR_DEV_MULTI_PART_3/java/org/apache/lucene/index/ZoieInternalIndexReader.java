package org.apache.lucene.index;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.internal.BaseSearchIndex;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.impl.indexing.internal.ReaderDirectory;
import proj.zoie.impl.indexing.internal.ReaderDirectory.ReaderEntry;

public class ZoieInternalIndexReader extends ZoieIndexReader
{
  private static final Logger log = Logger.getLogger(ZoieInternalIndexReader.class);
  
  public ZoieInternalIndexReader(IndexReader in) throws IOException
  {
    super(in);
  }
  
  @Override
  public void decRef() throws IOException {}
  
  @Override
  public void incRef() {}
  
  private static <R extends IndexReader> ReaderEntry<R> buildReaderEntry(SegmentReader sr,
                                                                         ReaderDirectory<R> directory,
                                                                         IndexReaderDecorator<R> decorator) throws IOException
  {
    String segName = sr.getSegmentName();
    ReaderEntry<R> readerEntry = (directory == null ? null : directory.getReaderEntry(segName));
    if (readerEntry == null)
    {
      readerEntry = new ReaderEntry<R>(new ZoieInternalIndexReader(sr),decorator);
    }
    else if(readerEntry.undecorated.numDeletedDocs() != sr.numDeletedDocs())
    {
      readerEntry = new ReaderEntry<R>(new ZoieInternalIndexReader(sr),decorator);
    }
    return readerEntry;
  }
  
  public static <R extends IndexReader> ReaderDirectory<R> buildReaderDirectory(IndexReader luceneReader,
                                                                                IndexReaderDecorator<R> decorator,
                                                                                IndexSignature sig,
                                                                                ReaderDirectory<R> oldDirectory) throws IOException
  {
    ReaderDirectory<R> readerDir = new ReaderDirectory<R>(sig);
    
    if (luceneReader instanceof SegmentReader)
    {
      SegmentReader sr = (SegmentReader)luceneReader;
      ReaderEntry<R> readerEntry = buildReaderEntry(sr,oldDirectory,decorator);
      readerDir.addReaderEntry(sr.getSegmentName(),readerEntry);
    }
    else if (luceneReader instanceof MultiSegmentReader)
    {
      MultiSegmentReader msr = (MultiSegmentReader)luceneReader;
      SegmentReader[] subreaders = msr.getSubReaders();
      for (SegmentReader sr : subreaders)
      {
        if(sr.numDocs() > 0)
        {
          ReaderEntry<R> readerEntry = buildReaderEntry(sr,oldDirectory,decorator);
          readerDir.addReaderEntry(sr.getSegmentName(),readerEntry);
        }
      }
    }
    else
    {
      throw new RuntimeException("cannot handle input reader: "+luceneReader.getClass());
    }
    return readerDir;
  }
  
  private static <R extends IndexReader>void deleteDocsForSegment(IndexReader luceneReader,
                                                                  SegmentReader segReader,
                                                                  int start,
                                                                  IntSet delSet,
                                                                  ReaderDirectory<R> readerDir) throws IOException
  {
    ReaderEntry<R> entry = null;
    if (readerDir!=null)
    {
      entry = readerDir.getReaderEntry(segReader.getSegmentName());
    }
    ZoieIndexReader zoieReader = (entry != null ? entry.undecorated : new ZoieIndexReader(segReader));
    
    int[] delArray = BaseSearchIndex.findDelDocIds(zoieReader,delSet);
    if (delArray != null && delArray.length > 0)
    {
      for (int docid : delArray)
      {
        // delete through the parent reader, otherwise segment info won't be updated
        luceneReader.deleteDocument(docid + start);
      }
    }
  }
  
  public static <R extends IndexReader> void deleteDocs(Directory dir,ReaderDirectory<R> readerDir,IntSet delSet) throws IOException
  {
    IndexReader luceneReader = null;
    try
    {
      luceneReader = IndexReader.open(dir);
      if (luceneReader instanceof SegmentReader)
      {
        SegmentReader segReader = (SegmentReader)luceneReader;
        deleteDocsForSegment(luceneReader,segReader,0,delSet,readerDir);
      }
      else if (luceneReader instanceof MultiSegmentReader)
      {
        MultiSegmentReader msr = (MultiSegmentReader)luceneReader;
        SegmentReader[] segReaders = msr.getSubReaders();
        int start = 0;
        for (SegmentReader segReader : segReaders)
        {
          deleteDocsForSegment(luceneReader,segReader,start,delSet,readerDir);
          start += segReader.maxDoc();
        }
      }
      else
      {
        throw new RuntimeException("cannot handle input reader: "+luceneReader.getClass());
      }
    }
    finally
    {
      if (luceneReader!=null)
      {
        luceneReader.close();
      }
    }
  }
}
