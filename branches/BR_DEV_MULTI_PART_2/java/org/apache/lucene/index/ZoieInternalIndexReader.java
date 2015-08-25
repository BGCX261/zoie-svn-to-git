package org.apache.lucene.index;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.internal.BaseSearchIndex;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.impl.indexing.internal.ReaderDirectory;
import proj.zoie.impl.indexing.internal.ReaderDirectory.ReaderEntry;

public class ZoieInternalIndexReader extends ZoieIndexReader{
	private static final Logger log = Logger.getLogger(ZoieInternalIndexReader.class);
	public ZoieInternalIndexReader(IndexReader in) throws IOException{
		super(in);
	}
	
	@Override
	public void decRef() throws IOException {			
	}

	@Override
	public void incRef() {
		
	}
	
	public void dispose() throws IOException
	{
		super.decRef();
	}
	
	protected void finalize(){
		try{
		  dispose();
		}
		catch(Exception e){
		  log.error(e.getMessage(),e);
		}
	}
	
	private static <R extends IndexReader> ReaderEntry<R> buildReaderEntry(SegmentReader sr,
			ReaderDirectory<R> directory,
			IndexReaderDecorator<R> decorator) throws IOException{
		String segName = sr.getSegmentName();
		ReaderEntry<R> readerEntry = directory == null ? null : directory.getReaderEntry(segName);
		if (readerEntry == null){
          readerEntry = new ReaderEntry<R>(new ZoieInternalIndexReader(sr),decorator);
		}
		else{
		  readerEntry.undecorated.in = sr;
		}
		return readerEntry;
	}
	
	private static <R extends IndexReader>void deleteDocsForSegment(IndexReader luceneReader,SegmentReader segReader,int start,IntSet delSet,ReaderDirectory<R> readerDir) throws IOException{
		
		ReaderEntry<R> entry = null;
		if (readerDir!=null){
			entry = readerDir.getReaderEntry(segReader.getSegmentName());
		}
		ZoieIndexReader zoieReader;
		if (entry!=null){
			zoieReader = entry.undecorated;
		}
		else{
			zoieReader = new ZoieIndexReader(segReader);
		}
		int[] delArray=BaseSearchIndex.findDelDocIds(zoieReader,delSet);
		if (delArray!=null && delArray.length > 0)
	    {
		  if (segReader!=null)
	      {
	        for (int docid : delArray)
	        {
	          // delete through the parent reader, otherwise segment info won't be updated
	          luceneReader.deleteDocument(docid + start);
	        }
	      }
	    }
	}
	
	public static <R extends IndexReader> void deleteDocs(Directory dir,ReaderDirectory<R> readerDir,IntSet delSet) throws IOException{
		IndexReader luceneReader = null;
		try{
			luceneReader = IndexReader.open(dir);
			if (luceneReader instanceof SegmentReader){
				SegmentReader segReader = (SegmentReader)luceneReader;
				deleteDocsForSegment(luceneReader,segReader,0,delSet,readerDir);
			}
			else if (luceneReader instanceof MultiSegmentReader){
				MultiSegmentReader msr = (MultiSegmentReader)luceneReader;
				SegmentReader[] segReaders = msr.getSubReaders();
				int start = 0;
				for (SegmentReader segReader : segReaders){
					deleteDocsForSegment(luceneReader,segReader,start,delSet,readerDir);
					start += segReader.maxDoc();
				}
			}
			else{
				throw new RuntimeException("cannot handle input reader: "+luceneReader.getClass());
			}
		}
		finally{
			if (luceneReader!=null){
				luceneReader.close();
			}
		}
	}
	
	public static <R extends IndexReader> ReaderDirectory<R> buildIndexReaders(IndexReader luceneReader,
			IndexReaderDecorator<R> decorator,
			IndexSignature sig,
			ReaderDirectory<R> oldDirectory) throws IOException{
		SortedMap<String,ReaderEntry<R>> map = new TreeMap<String,ReaderEntry<R>>();
		
		if (luceneReader instanceof SegmentReader){
			SegmentReader sr = (SegmentReader)luceneReader;
			ReaderEntry<R> readerEntry = buildReaderEntry(sr,oldDirectory,decorator);
			map.put(sr.getSegmentName(),readerEntry);
			return new ReaderDirectory<R>(sig,map);
		}
		else if (luceneReader instanceof MultiSegmentReader){
			MultiSegmentReader msr = (MultiSegmentReader)luceneReader;
			SegmentReader[] subreaders = msr.getSubReaders();
			for (SegmentReader sr : subreaders){
				ReaderEntry<R> readerEntry = buildReaderEntry(sr,oldDirectory,decorator);
				map.put(sr.getSegmentName(),readerEntry);
			}
			return new ReaderDirectory<R>(sig,map);
		}
		else{
			throw new RuntimeException("cannot handle input reader: "+luceneReader.getClass());
		}
	}
}
