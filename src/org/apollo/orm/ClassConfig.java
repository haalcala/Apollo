package org.apollo.orm;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.log4j.Logger;


class ClassConfig implements ApolloConstants {
	private static Logger logger = Logger.getLogger(ClassConfig.class);
	
	String cfName;
	
	Class<?> clazz;
	
	private Map<String, Map<String, String>> methodConfig;
	
	private Map<String, String> columnToMethodLookup;
	
	public static final String methodName = "methodName";
	public static final String methodType = "methodType";
	
	String idMethod;
	String idUnsaved;
	String idColumn;
	Class<TimeUUIDType> idGenerator;
	
	private List<String> indexMethods;
	
	Map<String, Class<?>> propertyType;
	
	Method idGetMethod;
	Method idSetMethod;
	
	private HashMap<String, Method> propGetMethods;
	private HashMap<String, Method> propSetMethods;
	
	boolean shouldProxy;
	
	List<Method> proxyMethods;
	
	Map<String, String> methodToProp;
	
	List<String> mapOfMaps;

	private ArrayList<String> colsAsArray;
	
	Serializable getIdValue(Object instance) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Serializable ret = null;
		
		if (idGetMethod == null)
			idGetMethod = clazz.getMethod(SessionImpl.getGetMethodFromProperty(idMethod), null);
		
		ret = (Serializable) idGetMethod.invoke(instance, null);
		
		return ret;
	}
	
	void setIdValue(Object instance, Serializable value) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		if (idSetMethod == null)
			idSetMethod = clazz.getMethod(SessionImpl.getSetMethodFromProperty(idMethod), new Class<?>[] {value.getClass()});
		
		idSetMethod.invoke(instance, new Object[] {value});
	}

	Method getGetMethodFromCache(String prop) throws SecurityException, NoSuchMethodException {
		Method method = propGetMethods == null ? null : propGetMethods.get(prop);
		
		if (method == null) {
			Class<?> type = propertyType.get(prop);

			if (type == Boolean.TYPE)
				method = clazz.getMethod(SessionImpl.getIsMethodFromProperty(prop), null);
			else
				method = clazz.getMethod(SessionImpl.getGetMethodFromProperty(prop), null);
			
			if (propGetMethods == null)
				propGetMethods = new HashMap<String, Method>();
			
			propGetMethods.put(prop, method);
		}
		
		return method;
	}
	
	Method getSetMethodFromCache(String prop) throws SecurityException, NoSuchMethodException {
		Method method = propSetMethods == null ? null : propSetMethods.get(prop);
		
		if (method == null) {
			method = clazz.getMethod(SessionImpl.getSetMethodFromProperty(prop), new Class<?>[] {getPropertyType(prop)});
			
			if (propSetMethods == null)
				propSetMethods = new HashMap<String, Method>();
			
			propSetMethods.put(prop, method);
		}
		
		return method;
	}
	
	Object getPropertyMethodValue(Object instance, String prop) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Method method = getGetMethodFromCache(prop);
		
		Object ret = method.invoke(instance, null);
		
		return ret;
	}
	
	void setPropertyMethodValue(Object instance, String prop, Object value) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Method method = getSetMethodFromCache(prop);
		
		method.invoke(instance, new Object[] {value});
	}

	void setMethodConfig(String propertyName, String config, String value) {
		if (propertyName == null || config == null || value == null)
			throw new NullPointerException();
		
		Map<String, String> propList = methodConfig == null ? null : methodConfig.get(propertyName);
		
		if (propList == null) {
			propList = new HashMap<String, String>();
			
			if (methodConfig == null) {
				methodConfig = new HashMap<String, Map<String,String>>();
				
				columnToMethodLookup = new HashMap<String, String>();
			}
			
			methodConfig.put(propertyName, propList);
		}
		
		if (config.equals("column") && columnToMethodLookup.get(value) == null) {
			if (logger.isDebugEnabled()) logger.debug("Caching column name: " + value + " with " + propertyName);
			columnToMethodLookup.put(value, propertyName);
		}
		
		if (!propList.keySet().contains(config)) {
			propList.put(config, value);
		}
	}
	
	Map<String, String> getMethodConfig(String propertyName) {
		return methodConfig.get(propertyName);
	}
	
	String getMethodUsingColumn(String columnName) {
		return columnToMethodLookup.get(columnName);
	}
	
	String getConfig(String propertyName, String config) {
		if (this.methodConfig == null)
			return null;
		
		Map<String, String> kvp = this.methodConfig.get(propertyName);
		
		return kvp == null ? null : kvp.get(config);
	}
	
	void validate() throws ApolloException {
		if (methodConfig == null)
			throw new ApolloException("Must define at least one method");
		
		if (methodConfig.get(idMethod) == null)
			throw new ApolloException("No id field defined");
	}
	
	Set<String> getMethods() {
		return methodConfig == null ? null : methodConfig.keySet();
	}
	
	public void setClazz(Class clazz) {
		this.clazz = clazz;
	}
	
	public Class getClazz() {
		return clazz;
	}
	
	public void setColumnFamily(String cfName) {
		this.cfName = cfName;
	}
	
	public String getColumnFamily() {
		return cfName;
	}
	
	public Class getPropertyType(String propertyName) {
		return propertyType.get(propertyName);
	}
	
	public ArrayList<String> getColumnsAsList() {
		if (colsAsArray == null) {
			colsAsArray = new ArrayList<String>();
			
			int colsi = 0;
			
			for (String prop : methodConfig.keySet()) {
				Map<String, String> method_config = methodConfig.get(prop);
				
				String column = method_config.get("column");
				
				if (column != null)
					colsAsArray.add(column);
			}
		}
		
		return colsAsArray;
	}
	
	public boolean isMapOfMaps(String prop) {
		if (mapOfMaps == null)
			return false;
		
		return mapOfMaps.contains(prop);
	}
	
	public String getMethodColumn(String prop, boolean proposeDefault) {
		String column = methodConfig.get(prop).get(ATTR_COLUMN);
		
		return column == null && proposeDefault ? prop : column;
	}
	
	public Class<?> getKeyType(String prop) throws ClassNotFoundException {
		Class type = getPropertyType(prop);
		
		if (type != Map.class && type != Set.class)
			if (type == null)
				throw new IllegalArgumentException("The property '" + prop + "' does not exist for this class '" + clazz + "'");
			else 
				throw new IllegalArgumentException("Propperty '" + prop + "' is neither a Map or Set type");
		
		Map<String, String> method_config = methodConfig.get(prop);
		
		if (method_config == null)
			throw new IllegalArgumentException("The property '" + prop + "' does not exist for this class '" + clazz + "'");
		
		String _key_type = method_config.get(ATTR_KEY_TYPE);
		
		Class<?> ret = _key_type != null ? Class.forName(_key_type) : String.class;
		
		return ret;
	}
	
	public String getMapKey(String prop, String rowKey) {
		String ret = null;
		
		Map<String, String> method_config = methodConfig.get(prop);
		
		if (method_config == null)
			throw new IllegalArgumentException("The property '" + prop + "' does not exist for this class '" + clazz + "'");
		
		String child_table_key_suffix = method_config.get(ATTR_CHILD_TABLE_KEY_SUFFIX);
		
		String child_table_key_pattern = method_config.get(ATTR_CHILD_TABLE_KEY_PATTERN);
		
		ret = Util.getMapKey(prop, rowKey, child_table_key_suffix, child_table_key_pattern);
		
		return ret;
	}

	public String getSetKey(String prop, String rowKey) {
		String ret = null;
		
		Map<String, String> method_config = methodConfig.get(prop);
		
		if (method_config == null)
			throw new IllegalArgumentException("The property '" + prop + "' does not exist for this class '" + clazz + "'");
		
		String child_table_key_suffix = method_config.get(ATTR_CHILD_TABLE_KEY_SUFFIX);
		
		ret = SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_KEY_COLLECTION_INDEX + "[" + rowKey + "|" + prop + (child_table_key_suffix != null ? "|" + child_table_key_suffix : "") + "]";
		
		return ret;
	}

	public String getListKey(String prop, String rowKey) {
		String ret = null;
		
		Map<String, String> method_config = methodConfig.get(prop);
		
		if (method_config == null)
			throw new IllegalArgumentException("The property '" + prop + "' does not exist for this class '" + clazz + "'");
		
		String child_table_key_suffix = method_config.get(ATTR_CHILD_TABLE_KEY_SUFFIX);
		
		ret = SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_KEY_COLLECTION_INDEX + "[" + rowKey + "|" + prop + (child_table_key_suffix != null ? "|" + child_table_key_suffix : "") + "]";
		
		return ret;
	}
	
	public Class<?> getValueType(String prop) throws ClassNotFoundException {
		Class<?> type = getPropertyType(prop);
		
		if (type != Map.class)
			if (type == null)
				throw new IllegalArgumentException("The property '" + prop + "' does not exist for this class '" + clazz + "'");
			else 
				throw new IllegalArgumentException("Propperty '" + prop + "' is neither a Map type");
		
		Map<String, String> method_config = methodConfig.get(prop);
		
		if (method_config == null)
			throw new IllegalArgumentException("The property '" + prop + "' does not exist for this class '" + clazz + "'");
		
		String _value_type = method_config.get(ATTR_VALUE_TYPE);
		
		Class<?> ret = _value_type != null ? Class.forName(_value_type) : String.class;
		
		return ret;
	}

	public boolean isLazyLoaded(String prop) {
		return Util.getBooleanValue(getMethodConfig(prop).get(ATTR_LAZY_LOADED), false);
	}
	
	public Class<?> getMethodParameterizedType(String prop) throws Exception {
		Method m = getGetMethodFromCache(prop);
		
		if (m.getGenericReturnType() instanceof ParameterizedType) {
			return (Class<?>) ((ParameterizedType) m.getGenericReturnType()).getActualTypeArguments()[0];
		}
		
		return null;
	}

	@Override
	public String toString() {
		return "clazz: " + clazz + " cfName: " + cfName + " idMethod: " + idMethod 
				+ " idUnsaved: " + idUnsaved + " idColumn: " + idColumn + " idGenerator: " 
				+ idGenerator + " indexMethods: " + indexMethods + " columnToMethodLookup: " + columnToMethodLookup + " methodConfig: " + methodConfig;
	}
}
