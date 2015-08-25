package proj.zoie.hibernate.index;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.hibernate.annotations.common.reflection.ReflectionManager;
import org.hibernate.annotations.common.reflection.XClass;
import org.hibernate.annotations.common.reflection.XMember;
import org.hibernate.annotations.common.reflection.java.JavaReflectionManager;
import org.hibernate.search.cfg.SearchConfiguration;
import org.hibernate.search.impl.InitContext;
import org.hibernate.search.util.ReflectionHelper;

import proj.zoie.api.indexing.Indexable;
import proj.zoie.api.indexing.IndexableInterpreter;
import proj.zoie.hibernate.cfg.ZoieSearchConfiguration;

public class HibernateIndexableInterpreter<V> implements IndexableInterpreter<V> {
	private final ZoieDocumentBuilderIndexedEntity<V> _documentBuilder;
	private final SearchConfiguration _conf;
	private HibernateIndexableInterpreter(SearchConfiguration conf,Class<V> cls){
		InitContext ctx = new InitContext(conf);
		_conf = conf;
		ReflectionManager refMgr = conf.getReflectionManager();
		if (refMgr == null){
			refMgr = new JavaReflectionManager();
		}
		@SuppressWarnings( "unchecked" )
		XClass mappedXClass = refMgr.toXClass(cls);
		
		_documentBuilder = new ZoieDocumentBuilderIndexedEntity<V>(
				mappedXClass, ctx, refMgr);
	}
	
	public Indexable interpret(V src) {
		XMember idGetter = _documentBuilder.getIdMember();
		Class typeClass = idGetter.getType().getClass();
	//	if (int.class.equals(typeClass) || Integer.TYPE.equals(typeClass)){
			final Integer id = (Integer)(ReflectionHelper.getMemberValue(src,idGetter));
			Map<String,String> fieldToAnalyzerMap = new HashMap<String,String>();
			final Document doc = _documentBuilder.getDocument(src, id,fieldToAnalyzerMap);
			doc.removeField(_documentBuilder.getIdKeywordName());
			return new Indexable(){

				public Document[] buildDocuments() {
					return new Document[]{doc};
				}

				public int getUID() {
					return id;
				}

				public boolean isDeleted() {
					return false;
				}

				public boolean isSkip() {
					return false;
				}
				
			};
	//	}
	//	else{
	//		throw new RuntimeException("id must be integer");
	//	}
	}
	
	public static <V >HibernateIndexableInterpreter<V> getHibernateIndexableInterpreter(ZoieSearchConfiguration conf,Class<V> cls){
		return new HibernateIndexableInterpreter<V>(conf,cls);
	}
}
