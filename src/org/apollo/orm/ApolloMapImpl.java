package org.apollo.orm;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apollo.orm.ApolloConstants.Util;
import org.springframework.beans.factory.annotation.Autowired;

public class ApolloMapImpl<T> implements Map<String, T> {
	private static Logger logger = Logger.getLogger(ApolloMap.class);

	private Map<String, T> map;
	private CassandraColumnFamilyWrapper cf;
	private SessionFactory factory;
	private String rowKey;
	
	private boolean mapOfMaps;

	private String prop;

	protected ClassConfig classConfig;

	private Session session;

	private String child_table_key_pattern;

	private String child_table_key_suffix;

	private String topKey;

	private String column;

	@Autowired
	public ApolloMapImpl(SessionFactory factory, String topKey, String rowKey, 
			CassandraColumnFamilyWrapper cf, String prop, Map<String, T> initialData, boolean mapOfMaps, ClassConfig classConfig) throws SecurityException, NoSuchMethodException {
		this.factory = factory;
		this.cf = cf;
		this.rowKey = rowKey;
		this.prop = prop;
		this.mapOfMaps = mapOfMaps;
		this.topKey = topKey;
		
		this.column = classConfig != null ? classConfig.getMethodColumn(prop, true) : prop;
		
		if (logger.isDebugEnabled())
			logger.debug("CassanateHashMap() rowKey: " + rowKey + " cf:" + cf + (cf != null ? " " + cf.getColumnFamilyName() : "") + " classConfig: " + classConfig);
		
		if (initialData != null && initialData.size() > 0) {
			for (String key : initialData.keySet()) {
				T _val = initialData.get(key);
				
				if (_val instanceof Map) {
					Map _mapOfMaps = (Map) _val;
					
					for (String _rowKey : initialData.keySet()) {
						Map<String, String> _map = (Map<String, String>) get(_rowKey);
						
						Map<String, String> __map = (Map<String, String>) initialData.get(_rowKey);
						
						if (logger.isDebugEnabled())
							logger.debug("_map: " + _map + " __map: " + __map);
						
						for (String col : __map.keySet()) {
							_map.put(col, __map.get(col));
						}
					}
				}
				if (_val instanceof String) {
					put(key, _val);
				}
			}
		}
	}
	
	public Map<String, T> getMap() {
		if (map == null)
			map = new LinkedHashMap<String, T>();
		
		return map;
	}

	public void clear() {
		getMap().clear();
	}

	public boolean containsKey(Object arg0) {
		return getMap().containsKey(arg0);
	}

	public boolean containsValue(Object value) {
		return getMap().containsValue(value);
	}

	public Set<java.util.Map.Entry<String, T>> entrySet() {
		return map.entrySet();
	}

	public T get(Object key) {
		if (logger.isDebugEnabled())
			logger.debug("CassanateHashMap.get key: " + key);
		
		Map<String, T> map = getMap();
		
		T ret = map.get(key);
		
		if (ret == null) {
			String mapKey = rowKey;
			
			if (mapOfMaps) {
				try {
					mapKey = (String) cf.getColumnValue(rowKey, column);
					
					if (mapKey == null) { 
						mapKey = rowKey + ":" + key;
					}
					
					//cf.insertColumn(rowKey, mapKey, "");
					
					cf.insertColumn(rowKey, (String) key, mapKey);
					
					if (logger.isDebugEnabled())
						logger.debug("mapKey: " + mapKey + " key: '" + key + "' rowKey: " + rowKey);
					
					ret = (T) new ApolloMapImpl<String>(factory, topKey, mapKey, cf, prop, null, false, null);
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			else {
				try {
					ret = (T) cf.getColumnValue(rowKey, key.toString());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			
			if (ret != null)
				map.put(key.toString(), ret);
		}
		
		return ret;
	}
	
	public boolean isEmpty() {
		return getMap().isEmpty();
	}

	public Set<String> keySet() {
		try {
			return new ApolloSetImpl<String>(factory.getSession(), prop, cf, classConfig, rowKey, "", "", 0);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public T put(String key, T value) {
		T ret = null;
		
		if (!mapOfMaps) {
			try {
				cf.insertColumn(rowKey, key, value.toString());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		else {
			T cols = (T) getMap().get(key.toString());
			
			if (cols == null) {
				String mapKey = classConfig.getMapKey(prop, rowKey);
				
				try {
					cols = (T) new ApolloMapImpl<String>(factory, null, mapKey, cf, prop, (Map<String, String>) value, false, null);

					value = cols;

					cf.insertColumn(rowKey, key, mapKey);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			
			((Map<String, T>) cols).put(key, value);
		}
		
		getMap().put(key, value);
		
		return ret;
	}

	public void putAll(Map<? extends String, ? extends T> m) {
		throw new RuntimeException("not implemented");
	}

	public T remove(Object key) {
		cf.deleteColumn(rowKey, key.toString());
		
		return getMap().remove(key);
	}

	public int size() {
		return getMap().size();
	}

	public Collection<T> values() {
		return getMap().values();
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
}
