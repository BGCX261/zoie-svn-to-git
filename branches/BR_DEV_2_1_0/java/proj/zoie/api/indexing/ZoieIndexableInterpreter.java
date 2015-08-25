package proj.zoie.api.indexing;

public interface ZoieIndexableInterpreter<V>{
	ZoieIndexable convertAndInterpret(V src);
}
