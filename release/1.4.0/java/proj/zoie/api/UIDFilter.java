package proj.zoie.api;

import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

public class UIDFilter extends Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final int[] _filteredIDs;

	public UIDFilter(int[] filteredIDs) {
		_filteredIDs = filteredIDs;
	}

	@Override
	public BitSet bits(IndexReader reader) throws IOException {
		if (reader instanceof ZoieIndexReader) {
			if (_filteredIDs == null) {
				throw new IllegalArgumentException(
						"filter doc id list array is null");
			}

			final int[] sorted = UIDDocIdSet.mapUID(_filteredIDs, ((ZoieIndexReader)reader).getDocIDMaper());
			if (sorted.length > 0) {
				BitSet bs = new BitSet(sorted[sorted.length - 1]);
				for (int id : sorted) {
					bs.set(id);
				}
				return bs;
			} else {
				return new BitSet(0);
			}
		} else {
			throw new IllegalArgumentException(
					"UIDFilter may only load from ZoieIndexReader instances");
		}
	}

	@Override
	public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
		if (reader instanceof ZoieIndexReader) {
			return new UIDDocIdSet(_filteredIDs, ((ZoieIndexReader)reader).getDocIDMaper());
		}
		else {
			throw new IllegalArgumentException(
			"UIDFilter may only load from ZoieIndexReader instances");
		}
	}
}
