package proj.zoie.api;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

public class UIDDocIdSet extends DocIdSet {
    private final int[] _sorted;
    
    public UIDDocIdSet(int[] uidArray,DocIDMapper mapper){
    	if (uidArray == null) throw new IllegalArgumentException("input uid array is null");
    	_sorted = mapUID(uidArray, mapper);
    }
    
    public UIDDocIdSet(int[] docids){
    	_sorted = docids;
    }
    
    public static int[] mapUID(int[] uidArray,DocIDMapper mapper)
	{
		IntRBTreeSet idSet = new IntRBTreeSet();
		for (int uid : uidArray)
		{
		  if (uid != ZoieIndexReader.DELETED_UID)
		  {
		    int docid = mapper.getDocID(uid);
		    if (docid!=DocIDMapper.NOT_FOUND)
		    {
		      idSet.add(docid);
	        }
	      }
		}
	    return idSet.toIntArray();
	}
    
	@Override
	public DocIdSetIterator iterator() {
		return new DocIdSetIterator(){
			int doc = -1;
			int current = -1;
			@Override
			public int doc() {
				return doc;
			}

			@Override
			public boolean next() throws IOException {
				if (current<_sorted.length-1){
					current++;
					doc = _sorted[current];
					return true;
				}
				return false;
			}

			@Override
			public boolean skipTo(int target) throws IOException {
				int idx = Arrays.binarySearch(_sorted,target);
				if (idx < 0)
				{
					idx = -(idx+1);
					if (idx>=_sorted.length) return false;
				}
				current = idx;
				doc = _sorted[current];
				return true;
			}
			
		};
	}

}
