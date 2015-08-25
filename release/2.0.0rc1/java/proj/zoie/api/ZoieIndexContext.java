package proj.zoie.api;

import it.unimi.dsi.fastutil.ints.IntSet;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieIndexContext {
	private ThreadLocal<IntSet> _delSet;
	private int _minUID;
	private int _maxUID;
	private IntSet _modifiedSet;
	private boolean _noDedup = false;
	private final IndexReaderDecorator<?> _decorator;
	
	ZoieIndexContext(IndexReaderDecorator<?> decorator){
		_decorator = decorator;
	}

	public ThreadLocal<IntSet> getDelSet() {
		return _delSet;
	}

	public void setDelSet(ThreadLocal<IntSet> delSet) {
		_delSet = delSet;
	}

	public int getMinUID() {
		return _minUID;
	}

	public void setMinUID(int minUID) {
		_minUID = minUID;
	}

	public int getMaxUID() {
		return _maxUID;
	}

	public void setMaxUID(int maxUID) {
		_maxUID = maxUID;
	}

	public IntSet getModifiedSet() {
		return _modifiedSet;
	}

	public void setModifiedSet(IntSet modifiedSet) {
		_modifiedSet = modifiedSet;
	}

	public boolean isNoDedup() {
		return _noDedup;
	}

	public void setNoDedup(boolean noDedup) {
		_noDedup = noDedup;
	}

	public IndexReaderDecorator<?> getDecorator() {
		return _decorator;
	}
}
