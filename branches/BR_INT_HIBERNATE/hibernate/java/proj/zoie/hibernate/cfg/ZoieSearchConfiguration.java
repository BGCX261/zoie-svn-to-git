package proj.zoie.hibernate.cfg;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.hibernate.annotations.common.reflection.ReflectionManager;
import org.hibernate.search.cfg.SearchConfiguration;

public class ZoieSearchConfiguration implements SearchConfiguration {
	private ReflectionManager _reflectionManager = null;
	private final Properties _props;
	private final Map<String,Class<?>> _classMap;
	public ZoieSearchConfiguration(Properties props,Map<String,Class<?>> classMap){
		_props = props;
		_classMap = classMap;
	}
	
	public void setReflectionManager(ReflectionManager reflectionManager){
		_reflectionManager = reflectionManager;
	}
	
	public Class<?> getClassMapping(String name) {
		return _classMap == null ? null : _classMap.get(name);
	}

	public Iterator<Class<?>> getClassMappings() {
		return _classMap == null ? null : _classMap.values().iterator();
	}

	public Properties getProperties() {
		return _props;
	}

	public String getProperty(String propName) {
		return _props == null ? null : _props.getProperty(propName);
	}

	public ReflectionManager getReflectionManager() {
		return _reflectionManager;
	}
}
