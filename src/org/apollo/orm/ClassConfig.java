package org.apollo.orm;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.log4j.Logger;


class ClassConfig {
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
	
	public boolean isMapOfMaps(String prop) {
		if (mapOfMaps == null)
			return false;
		
		return mapOfMaps.contains(prop);
	}

	@Override
	public String toString() {
		return "clazz: " + clazz + " cfName: " + cfName + " idMethod: " + idMethod 
				+ " idUnsaved: " + idUnsaved + " idColumn: " + idColumn + " idGenerator: " 
				+ idGenerator + " indexMethods: " + indexMethods + " columnToMethodLookup: " + columnToMethodLookup + " methodConfig: " + methodConfig;
	}
}
