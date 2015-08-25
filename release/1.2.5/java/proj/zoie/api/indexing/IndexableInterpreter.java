package proj.zoie.api.indexing;

public interface IndexableInterpreter<V>{
	Indexable interpret(V src);
}
