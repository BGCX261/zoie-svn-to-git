package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public final class ReaderDirectory<R extends IndexReader> {
	public static final class ReaderEntry<R extends IndexReader>{
		public final ZoieIndexReader undecorated;
		public final R decorated;
		public ReaderEntry(ZoieIndexReader undecorated,R decorated){
			this.undecorated = undecorated;
			this.decorated = decorated;
		}
		
		public ReaderEntry(ZoieIndexReader undecorated,IndexReaderDecorator<R> decorator) throws IOException{
			this.undecorated = undecorated;
			this.decorated = decorator.decorate(undecorated);
		}
		
		@Override
		public String toString(){
			StringBuffer buf = new StringBuffer();
			buf.append("undecorated: ").append(undecorated);
			buf.append("\ndecorated: ").append(decorated);
			return buf.toString();
		}
	}
	private final IndexSignature _sig;
	private final SortedMap<String,ReaderEntry<R>> _directoryMap;
	
	public ReaderDirectory(IndexSignature sig,SortedMap<String,ReaderEntry<R>> directoryMap){
		_sig = sig;
		_directoryMap = directoryMap;
	}
	
	public void setDeleteSet(IntSet delSet){
		Collection<ReaderEntry<R>> entries = getIndexReaderEntries();
		Iterator<ReaderEntry<R>> iter = entries.iterator();
		
		while (iter.hasNext()){
			iter.next().undecorated.setDelSet(delSet);
		}
	}
	
	public ReaderEntry<R> getReaderEntry(String segmentName){
		return _directoryMap == null ? null : _directoryMap.get(segmentName);
	}
	
	public int getNumDocs(){
		int numDocs = 0;
		if (_directoryMap!=null){
			Iterator<ReaderEntry<R>> iter = _directoryMap.values().iterator();
			while(iter.hasNext()){
				ReaderEntry<R> entry = iter.next();
				numDocs+=entry.undecorated.numDocs();
			}
		}
		return numDocs;
	}
	
	public SortedMap<String,ReaderEntry<R>> getDirectoryMap(){
		return _directoryMap;
	}
	
	public IndexSignature getSignature(){
		return _sig;
	}
	
	public Collection<R> getReaderCollection(){
		Collection<ReaderEntry<R>> entries = getIndexReaderEntries();
		ArrayList<R> readerList = new ArrayList<R>(entries.size());
		
		for (ReaderEntry<R> entry: entries){
			readerList.add(entry.decorated);
		}
		return readerList;
	}
	
	public Collection<ReaderEntry<R>> getIndexReaderEntries(){
		if (_directoryMap!=null){
			return _directoryMap.values();
		}
		else{
			return new ArrayList<ReaderEntry<R>>(0);
		}
	}
	
	public void dispose(){
		if (_directoryMap!=null){
			_directoryMap.clear();
		}
	}
}
