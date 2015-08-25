package proj.zoie.api.indexing;

public interface ZoieIndexableInterpreter<V> extends IndexableInterpreter<V> {
	ZoieIndexable convertAndInterpret(V src);
}
