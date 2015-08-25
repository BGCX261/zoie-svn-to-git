package proj.zoie.api.indexing;

/**
 * @deprecated please use {@link ZoieIndexableInterpreter}
 * @author john
 *
 * @param <V>
 */
public interface IndexableInterpreter<V>{
	/**
	 * @deprecated please see {@link ZoieIndexableInterpreter#convertAndInterpret(Object)}
	 * @param src
	 * @return
	 */
	Indexable interpret(V src);
}
