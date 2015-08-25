package proj.zoie.hibernate.index;

import org.hibernate.annotations.common.reflection.ReflectionManager;
import org.hibernate.annotations.common.reflection.XClass;
import org.hibernate.annotations.common.reflection.XMember;
import org.hibernate.search.engine.DocumentBuilderIndexedEntity;
import org.hibernate.search.impl.InitContext;
import org.hibernate.search.store.DirectoryProvider;

public class ZoieDocumentBuilderIndexedEntity<T> extends
		DocumentBuilderIndexedEntity<T> {

	public ZoieDocumentBuilderIndexedEntity(XClass clazz, InitContext context, ReflectionManager reflectionManager) {
		super(clazz, context, new DirectoryProvider[0],null, reflectionManager);
	}
	
	public XMember getIdMember(){
		return idGetter;
	}
}
