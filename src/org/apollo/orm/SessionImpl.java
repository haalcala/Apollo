package org.apollo.orm;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import net.sf.cglib.proxy.Enhancer;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.apache.log4j.Logger;

public class SessionImpl implements Session, ApolloConstants {
	private static Logger logger = Logger.getLogger(SessionImpl.class);
	
	private static final String CACHE_NAME = "apollo";

	private SessionFactory factory;
	
	private CassandraKeyspaceWrapper keyspaceWrapper;
	
	private Map<Class<?>, ClassConfig> classToClassConfig;
	
	private Map<String, ClassConfig> columnFamilyToClassConfig;
	
	Map<String, List<String>> lazyLoadedProps;

	public SessionImpl(SessionFactory factory, CassandraKeyspaceWrapper keyspaceWrapper
			, Map<Class<?>, ClassConfig> classToClassConfig, Map<String, ClassConfig> columnFamilyToClassConfig) {
		this.factory = factory;
		this.keyspaceWrapper = keyspaceWrapper;
		this.classToClassConfig = classToClassConfig;
		this.columnFamilyToClassConfig = columnFamilyToClassConfig;
	}

	public ClassConfig getClassConfig(Class<?> clazz) {
		return getClassConfig(clazz, false);
	}

	public <T> T get(Class<T> clazz, Serializable id) throws ApolloException {
		if (clazz == null || id == null)
			throw new IllegalArgumentException("Neither class '" + clazz + "' nor id '" + id + "' cannot be null");
		
		ClassConfig cc = getClassConfigUsingClass(clazz);
		
		CassandraColumnFamilyWrapper cf = getColumnFamilyUsingClassConfig(cc);
		
		try {
			T ret = getEntityUsingId(cf, id, cc, null);
			
			if (ret == null)
				throw new IllegalAccessException("Object not found.");
			
			return ret;
		} catch (Exception e) {
			throw new ApolloException(e);
		}
	}

	public <T> T getEntityUsingId(CassandraColumnFamilyWrapper cf, Serializable id, ClassConfig classConfig, Object inverse) throws Exception {
		Map<String, Map<String, Serializable>> rows = cf.getColumnsAsMap(id.toString(), "", "", "", 100, 1);
		
		if (rows != null && rows.size() > 0) {
			Map<String, Serializable> cols = rows.get(id);
			
			if (cols == null || (cols != null && cols.size() == 0)) {
				if (logger.isDebugEnabled())
					logger.debug("Returning null coz there's no column found");
				
				return null;
			}
			
			return colsToObject(id.toString(), cols, classConfig, inverse);
		}
		
		return null;
	}
	
	static String getSetMethodFromProperty(String propertyName) {
		return "set" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
	}

	static String getGetMethodFromProperty(String propertyName) {
		return "get" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
	}
	
	static String getIsMethodFromProperty(String propertyName) {
		return "is" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
	}
	
	<T> T colsToObject(String idValue, Map<String, Serializable> cols, ClassConfig classConfig, Object inverse) throws Exception {
		T ret = (T) classConfig.clazz.newInstance();
		
		classConfig.setIdValue(ret, idValue);

		setObjectToCache(ret, idValue);
		
		for (String prop : classConfig.getMethods()) {
			if (logger.isDebugEnabled())
				logger.debug("######## Processing property: " + prop + " with id: " + idValue);
			
			ret = doPropertyInjection(idValue, cols, classConfig, ret, prop, false);
			
			if (ret == null) {
				logger.warn("Returning null coz doPropertyInjection method returned 'null' which is usually caused by missing required fields.");
				
				removeObjectFromCache(classConfig.clazz, idValue);
				return null;
			}
		}
		
		return ret;
	}

	<T> T doPropertyInjection(String idValue, Map<String, Serializable> cols, ClassConfig classConfig, T ret, String prop, boolean force)
			throws Exception, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		Map<String, String> method_config = classConfig.getMethodConfig(prop);
		
		Class<?> type = classConfig.getPropertyType(prop);
		
		if (type == Set.class) {
			commenceSetPropertyInjection(ret, prop, classConfig);
		}
		else if (type == Map.class) {
			String child_table = method_config.get(ATTR_TABLE);
			
			child_table = child_table == null ? classConfig.getColumnFamily() : child_table;
			
			boolean lazy_loaded = classConfig.isLazyLoaded(prop);
			
			boolean mapOfMaps = classConfig.isMapOfMaps(prop);

			String columnIdValue = (String) cols.get(classConfig.getMethodColumn(prop, true));

			if (columnIdValue == null) classConfig.getMapKey(prop, idValue);

			CassandraColumnFamilyWrapper child_table_cf = factory.getCassandraColumnFamilyWrapper(child_table);
			
			Class<?> key_type = classConfig.getKeyType(prop);
			Class<?> value_type = classConfig.getValueType(prop);

			Map map = null;

			if (lazy_loaded) {
				if (mapOfMaps) {
					map = new ApolloMapImpl<Map<Object, Object>>(factory, idValue, columnIdValue, child_table_cf, prop, null, mapOfMaps, classConfig);
				}
				else {
					map = new ApolloMapImpl<Object>(factory, idValue, columnIdValue, child_table_cf, prop, null, mapOfMaps, classConfig);
				}
			}
			else {
				map = new LinkedHashMap<Object, Object>();

				Iterator<String> it = new ApolloKeyIteratorImpl(child_table_cf, columnIdValue, 1, "", "", 0, MAX_COLUMN_PER_PAGE, false);

				while (it.hasNext()) {
					String key = it.next();

					if (!key.startsWith("__") && !key.endsWith("__")) {
						String val = (String) child_table_cf.getColumnValue(columnIdValue, key);

						if (mapOfMaps) {
							Map<String, Map<String, Serializable>> inside_map = child_table_cf.getColumnsAsMap(val, "", "", "", Integer.MAX_VALUE, 1);
							
							Map<String, Serializable> this_cols = inside_map.get(val);
							
							if (this_cols != null)
								map.put(key, this_cols);
						}
						else {
							map.put(key, val);
						}
					}
				}
			}

			classConfig.setPropertyMethodValue(ret, prop, map);
			
			/*
			if (table == null) {
				// commenceNativeMapPropertyInjection(ret, cols, prop, classConfig);
				CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(classConfig.cfName);
				
				String mapKey = cols.get(method_config.get("column"));
				
				if (logger.isDebugEnabled())
					logger.debug("mapKey: " + mapKey);
				
				Map<String, Map<String, String>> rows = cf.getColumnsAsMap(mapKey, "", "", "", 100, 1);
				
				if (rows != null)
					classConfig.setPropertyMethodValue(ret, prop, rows.get(mapKey));
			}
			else {
				boolean mapOfMaps = classConfig.isMapOfMaps(prop);
				
				String child_table_key_suffix = method_config.get(SessionImpl.ATTR_CHILD_TABLE_KEY_SUFFIX);

				String columnIdValue = idValue + (child_table_key_suffix != null ? child_table_key_suffix : "");

				CassandraColumnFamilyWrapper child_table_cf = factory.getCassandraColumnFamilyWrapper(table);

				ApolloMapImpl<?> map;
				
				if (mapOfMaps) {
					map = new ApolloMapImpl<Map<String, String>>(factory, columnIdValue, child_table_cf, prop, null, mapOfMaps, null);
				}
				else {
					map = new ApolloMapImpl<String>(factory, columnIdValue, child_table_cf, prop, null, mapOfMaps, null);
				}
				
				classConfig.setPropertyMethodValue(ret, prop, map);
			}
			/*
			else {
				boolean lazy_loaded = _lazy_loaded != null && _lazy_loaded.equalsIgnoreCase("true");
				
				if (force || !lazy_loaded) {
					if (logger.isDebugEnabled()) logger.debug("'table' attribute detected for '" + prop + "'");
					CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(table);

					String child_table_key_suffix = method_config.get(ATTR_CHILD_TABLE_KEY_SUFFIX);

					Map<String, Map<String, String>> rows = cf.getColumnsAsMap(idValue + (child_table_key_suffix != null ? child_table_key_suffix : ""), "", "", "", 100, 1);

					if (rows != null) {
						for (String rowKey : rows.keySet()) {
							Map<String, String> map_cols = rows.get(rowKey);

							classConfig.setPropertyMethodValue(ret, prop, map_cols);

							break;
						}
					}
				}
				else {
					if (logger.isDebugEnabled()) logger.debug("Skipping property '" + prop + "' for lazy loading.");
				}
			}
			*/
		}
		else if (type == List.class) {
			String _class = method_config.get(ATTR_VALUE_TYPE);

			Class<?> value_type = _class == null ? String.class : Class.forName(_class);

			String table = method_config.get(ATTR_TABLE);

			if (table == null) table = classConfig.cfName;

			CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(table);

			String listKey = classConfig.getListKey(prop, idValue);

			Object val = cols.get(method_config.get(ATTR_COLUMN));
			
			if (classConfig.isLazyLoaded(prop)) {
				val = new ApolloListImpl<T>(this, prop, cf, classConfig, listKey, "", "", MAX_COLUMN_PER_PAGE);
			}
			else {
				List<String> _list = new ApolloListImpl<String>(this, prop, cf, classConfig, listKey, "", "", MAX_COLUMN_PER_PAGE);
				
				List<Object> list = null;
				
				for (String _val : _list) {
					if (list == null)
						list = new ArrayList<Object>();
					
					list.add(Util.getNativeValueFromString(value_type, _val));
				}
				
				val = list;
			}
			
			classConfig.setPropertyMethodValue(ret, prop, val);
		}
		else {
			Serializable val = cols.get(method_config.get(ATTR_COLUMN));
			
			if (val != null) {
				if (type == Timestamp.class)
					val = new Timestamp(((Date) val).getTime());
				else if (!Util.isNativelySupported(type))
					val = (Serializable) find(type, val);
			}
			
			String not_null = method_config.get(ATTR_NOT_NULL);
			
			if (val == null && not_null != null && not_null.equals("true")) {
				logger.warn("Null value detected for not-null property '" + prop + "' col: " + method_config.get(ATTR_COLUMN));
				
				// throw new IllegalAccessException("Null cannot be assigned to property '" + prop + "' with 'not-null' attribute");
				return null;
			}

			if (val != null)
				setPropertyMethodValue(ret, val, prop, classConfig);
		}
		
		return ret;
	}

	private void commenceNativeMapPropertyInjection(Object ret, Map<String, String> cols, String prop, ClassConfig classConfig) throws Exception {
		Map<Object, Object> map = (Map<Object, Object>) classConfig.getPropertyMethodValue(ret, prop);
		
		String _key_type = classConfig.getMethodConfig(prop).get(ATTR_KEY_TYPE);
		String _value_type = classConfig.getMethodConfig(prop).get(ATTR_VALUE_TYPE);
		
		Class<?> key_type = _key_type != null ? Class.forName(_key_type) : String.class;
		Class<?> value_type = _value_type != null ? Class.forName(_value_type) : String.class;
		
		for (String col : cols.keySet()) {
			String _val = cols.get(col);
			
			Object key = null;
			Object value = null;
			
			if (col.startsWith("__") && col.endsWith("__"))
				continue;
			
			if (map == null)
				map = new LinkedHashMap<Object, Object>();
			
			if (key_type == Integer.TYPE) {
				key = Integer.parseInt((String) col);
			}
			else if (key_type == Long.TYPE) {
				key = Long.parseLong((String) col);
			}
			else if (key_type == Double.TYPE) {
				key = Double.parseDouble((String) col);
			}
			else if (key_type == Float.TYPE) {
				key = Float.parseFloat((String) col);
			}
			else if (key_type == Boolean.TYPE) {
				key = Boolean.parseBoolean((String) col);
			}
			else if (key_type == Short.TYPE) {
				key = Short.parseShort((String) col);
			}
			else if (key_type == Byte.TYPE) {
				key = Byte.parseByte((String) col);
			}
			else if (key_type == String.class) {
				key = col;
			}
			else if (key_type == Timestamp.class) {
				key = Util.getTimestamp(col);
			}
			

			if (value_type == Integer.TYPE) {
				value = Integer.parseInt((String) _val);
			}
			else if (value_type == Long.TYPE) {
				value = Long.parseLong((String) _val);
			}
			else if (value_type == Double.TYPE) {
				value = Double.parseDouble((String) _val);
			}
			else if (value_type == Float.TYPE) {
				value = Float.parseFloat((String) _val);
			}
			else if (value_type == Boolean.TYPE) {
				value = Boolean.parseBoolean((String) _val);
			}
			else if (value_type == Short.TYPE) {
				value = Short.parseShort((String) _val);
			}
			else if (value_type == Byte.TYPE) {
				value = Byte.parseByte((String) _val);
			}
			else if (value_type == String.class) {
				value = _val;
			}
			else if (value_type == Timestamp.class) {
				value = Util.getTimestamp(_val);
			}
			
			if (key == null || value == null)
				throw new IllegalArgumentException("Either of the key '" + col + "' or value '" + _val + "'" +
						" could not be converted to '" + key_type + "' or '" + value_type + "' respectively.");

			map.put(key, value);
		}
		
		if (map != null && map.size() > 0)
			classConfig.setPropertyMethodValue(ret, prop, map);
	}

	private void setPropertyMethodValue(Object ret, Serializable val, String prop, ClassConfig classConfig)
			throws Exception {
		Class<?> paramType = classConfig.getPropertyType(prop);

		Object value = null;
		
		if (paramType == Integer.TYPE) {
			value = val;
		}
		else if (paramType == Long.TYPE) {
			value = val;
		}
		else if (paramType == Double.TYPE) {
			value = val;
		}
		else if (paramType == Float.TYPE) {
			value = val;
		}
		else if (paramType == Boolean.TYPE) {
			value = val;
		}
		else if (paramType == Short.TYPE) {
			value = val;
		}
		else if (paramType == Byte.TYPE) {
			value = val;
		}
		else if (paramType == String.class) {
			value = val;
		}
		else if (paramType == Timestamp.class) {
			if (val != null)
				value = new Timestamp(((Date) val).getTime());
		}
		else if (paramType == Map.class) {
			if (val != null && val.getClass() == String.class) {
				Map<String, String> _val = stringToMap("|", "=", (String) val);

				value =  _val;
			}
			else if (val.getClass() == Map.class) {
				value = val;
			}
		}
		else if (paramType == List.class) {
			value = csvToList((String) val);
		}
		else {
			ClassConfig cc = classToClassConfig.get(paramType);
			
			if (cc == null && cc.clazz.getSuperclass() != Object.class)
				cc = classToClassConfig.get(paramType);

			if (cc == null)
				throw new IllegalArgumentException("No mapping defined for '" + paramType + "'");
			
			String idValue = (String) cc.getIdValue(val);
			
			value = getObjectFromCache(paramType, idValue);
			
			if (value == null) {
				value = get(paramType, (Serializable) val);

				setObjectToCache(value, (String) val);
			}
		}
		
		//if (logger.isDebugEnabled())
		//	logger.debug("Setting property '" + prop + "' value '" + value + "' to return object '" + ret + "'");

		classConfig.setPropertyMethodValue(ret, prop, value);
	}

	private void commenceSetPropertyInjection(Object o, String prop, ClassConfig classConfig) throws Exception {
		if (logger.isDebugEnabled()) logger.debug("Processing a property "+prop+" with 'Set' type.");
		
		Class<?> generic_type = classConfig.getMethodParameterizedType(prop);
		
		boolean isNative = Util.isNativelySupported(generic_type);
		
		Map<String, String> methodConfig = classConfig.getMethodConfig(prop);
		
		String _class = methodConfig.get(ATTR_CLASS);
		String table = methodConfig.get(ATTR_TABLE);
		
		//Class<?> clazz = Class.forName(_class);
		
		Object oidValue = classConfig.getIdValue(o);
		
		String idValue = oidValue == null ? null : oidValue.toString();
		
		CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(classConfig.cfName);
		
		String child_table_rowKey = (String) cf.getColumnValue(idValue, classConfig.getMethodColumn(prop, true));
		
		if (logger.isDebugEnabled())
			logger.debug("child_table_rowKey: " + child_table_rowKey);
		
		CassandraColumnFamilyWrapper child_table_cf = table != null ? factory.getCassandraColumnFamilyWrapper(table) : cf;
		
		//Map<String, Map<String, String>> rows = child_table_cf.getColumnsAsMap(child_table_rowKey, "", "colId_0", "", 100, 1);

		//Map<String, String> cols = rows.get(child_table_rowKey);
		
		Set set = new ApolloSetImpl(this, prop, child_table_cf, classConfig, child_table_rowKey, "", "", 0);

		/*
		if (cols != null) {
			for (int i = 0; i < lastId; i++) {
				String otherKey = cols.get("colId_" + i);

				if (logger.isDebugEnabled()) 
					logger.debug("Trying to find class " + clazz + " using key " + otherKey + " with parent key " + idValue);

				Object object = find(clazz, otherKey);

				if (logger.isDebugEnabled())
					logger.debug("Class '" + clazz + "' with key '" + otherKey + "' object: " + object);

				if (object != null) {
					if (set == null)
						set = new HashSet();

					set.add(object);
				}
			}
		}
		*/

		if (set != null) {
			classConfig.setPropertyMethodValue(o, prop, set);
		}
	}
	
	public <T> T find(Class<T> clazz, Serializable id) throws ApolloException {
		if (clazz == null || id == null)
			throw new NullPointerException();
		
		try {
			return find(clazz, id, null);
		} catch (ApolloException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public <T> T find(Class<T> clazz, Serializable id, Serializable inverse) throws ApolloException {
		if (logger.isDebugEnabled())
			logger.debug("********************* START:: trying to find class '" + clazz + "' with id '" + id + "'  *********************");
		
		ClassConfig cc = getClassConfigUsingClass(clazz);
		
		if (cc == null)
			throw new ApolloException("Class " + clazz + " has not been registered.");
		
		CassandraColumnFamilyWrapper cf = getColumnFamilyUsingClassConfig(cc);
		
		try {
			return getEntityUsingId(cf, id, cc, null);
		} catch (Exception e) {
			throw new ApolloException(e);
		}
		finally {
			if (logger.isDebugEnabled())
				logger.debug("********************* END::  trying to find class '" + clazz + "' with id '" + id + "'  *********************");
		}
	}

	CassandraColumnFamilyWrapper getColumnFamilyUsingClassConfig(ClassConfig cc) {
		CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(cc.cfName);
		return cf;
	}

	ClassConfig getClassConfigUsingClass(Class clazz) throws ApolloException {
		ClassConfig cc = classToClassConfig.get(clazz);
		
		if (cc == null)
			throw new ApolloException("Class " + clazz + " has not been registered.");
		
		return cc;
	}

	public <T> List<T> list(Class<T> clazz) throws ApolloException {
		return new CriteriaImpl<T>(this, clazz).list();
	}

	public List<String> getKeyList(CassandraColumnFamilyWrapper columnFamilyWrapper, String startKey) throws Exception {
		return getKeyList(columnFamilyWrapper, startKey, 0);
	}
	
	public List<String> getKeyList(CassandraColumnFamilyWrapper columnFamilyWrapper, String startKey, int rowCount) throws Exception {
		return getKeyList(columnFamilyWrapper, startKey, null, rowCount);
	}
	
	public List<String> getKeyList(CassandraColumnFamilyWrapper columnFamilyWrapper, String startKey, String[] super_columns, int maxRows) throws Exception {
		List<String> ret = null;
		
		Map<String, Map<String, Serializable>> rows = columnFamilyWrapper.getColumnsAsMap(startKey, "", "", "", 1, maxRows, true);

		if (rows != null) {
			ret = new ArrayList<String>(rows.keySet());
		}
		
		return ret;
	}
	
	public static Map<String, String> stringToMap(String rowDelimeter, String colDelimeter, String data) {
		Map<String, String> ret = null;
		
		if (data != null) {
			StringTokenizer st = new StringTokenizer(data, rowDelimeter);
			
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				
				StringTokenizer st2 = new StringTokenizer(tok, colDelimeter);
				
				String key = st2.hasMoreTokens() ? st2.nextToken() : null;
				String filter = st2.hasMoreTokens() ? st2.nextToken() : null;
				
				if (key == null || filter == null) {
					continue;
				}
				
				if (ret == null)
					ret = new LinkedHashMap<String, String>();
				
				ret.put(key, filter);
			}
		}
		
		return ret;
	}
	
	public static String mapToStringDelimited(String rowDelimeter, String colDelimeter, Map<String, String> data) {
		String dat = "";
		
		for (String key : data.keySet()) {
			dat += (!dat.equals("") ? rowDelimeter : "") + key + colDelimeter + data.get(key);
		}
		
		return !dat.equals("") ? dat : null;
	}
	
	public static List<String> csvToList(String csv) {
		List<String> ret = null;
		
		if (csv != null && csv.length() > 0) {
			StringTokenizer st = new StringTokenizer(csv, ",");
			
			while (st.hasMoreTokens()) {
				if (ret == null)
					ret = new ArrayList<String>();
				
				ret.add(st.nextToken());
			}
		}
		
		return ret;
	}
	
	public static String listToCSV(List<?> list) {
		String ret = "";
		
		for (Object o : list) {
			ret += (ret.equals("") ? "" : ",") + o.toString();
		}
		
		return ret;
	}

	public <T> T save(final T object) throws ApolloException {
		if (object == null)
			throw new NullPointerException();
		
		if (logger.isDebugEnabled()) 
			logger.debug("*********************   Saving class "+object.getClass()+" object " + object + " **************************");
		
		Map<String, List<String>> no_auto_delete_list = new LinkedHashMap<String, List<String>>();
		
		try {
			ClassConfig cc = getClassConfig(object);
			
			final CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(cc.getColumnFamily());
			
			Map<String, Map<String, Map<String, Serializable>>> cfsToSave = new LinkedHashMap<String, Map<String,Map<String,Serializable>>>();
			
			String idValue = (String) cc.getIdValue(object);

			if (idValue == cc.idUnsaved) {
				idValue = TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString();

				cc.setIdValue(object, idValue);
			}
			
			if (logger.isDebugEnabled()) logger.debug("idValue : " + idValue);
			
			//Object cached = getObjectFromCache(cc.clazz, idValue);
			
			// object = cached != null ? cached : object;
			
			if (logger.isDebugEnabled()) logger.debug(object.getClass() + " Object = " + object);
			
			Set<String> props = cc.getMethods();
			
			for (String prop : props) {
				Map<String, String> propConfig = cc.getMethodConfig(prop);
				
				String column = propConfig.get(ATTR_COLUMN);
				
				//if (logger.isDebugEnabled()) logger.debug("Column: " + column);
				
				String method_name = getGetMethodFromProperty(prop);
				
				//if (logger.isDebugEnabled()) logger.debug("method_name: " + method_name);
				
				Serializable value = null;
				
				Class<?> returnType = cc.getPropertyType(prop);
				
				//if (logger.isDebugEnabled()) logger.debug("returnType: " + returnType);
				
				boolean checkForNull = true;
				
				boolean not_null = Util.getBooleanValue(propConfig.get(ATTR_NOT_NULL), false);
				
				if (logger.isDebugEnabled())
					logger.debug("########### Processing property: " + prop + " column: " + column + " type: " + returnType);
				
				if (returnType == Integer.TYPE
						|| returnType == Long.TYPE
						|| returnType == Double.TYPE
						|| returnType == Float.TYPE
						|| returnType == Short.TYPE
						|| returnType == Boolean.TYPE
						|| returnType == Byte.TYPE
						|| returnType == String.class) {
					value = cc.getPropertyMethodValue(object, prop);
				}
				else if (returnType == Timestamp.class) {
					Timestamp ts = (Timestamp) cc.getPropertyMethodValue(object, prop);

					value = ts;
				}
				else if (returnType == Map.class) {
					// if (logger.isDebugEnabled()) logger.debug("Saving property '" + prop + "' of type " + Map.class);
					
					/*
					 * Check if there's any table specified
					 */
					String child_table = propConfig.get(ATTR_TABLE);
					
					child_table = child_table != null ? child_table : cc.getColumnFamily();
					
					Map<?, ?> map = (Map<?, ?>) cc.getPropertyMethodValue(object, prop);
					
					String mapKey = cc.getMapKey(prop, idValue);
					
					value = map != null ? mapKey : null;
					
					boolean in_no_auto_delete = addToNoAutoDelete(no_auto_delete_list, propConfig, child_table, mapKey);
					
					addToDataToSave(cc.cfName, cfsToSave, idValue, column, mapKey);
					
					boolean mapOfMaps = cc.isMapOfMaps(prop);
						
					if (!(map instanceof ApolloMapImpl<?>)) {
						if (logger.isDebugEnabled()) logger.debug("map : " + map);

						if (map != null && map.size() > 0) {
							for (Object key : map.keySet()) {
								if (mapOfMaps) {
									Map<String, String> inside_map = (Map<String, String>) map.get(key);
									
									if (inside_map != null) {
										for (String inside_mapKey : inside_map.keySet()) {
											String val = inside_map.get(inside_mapKey);
											
											String this_rowKey = cc.getMapKey(prop, idValue) + ":" + key.toString();
											
											if (in_no_auto_delete)
												addToNoAutoDelete(no_auto_delete_list, propConfig, child_table, this_rowKey);
											
											addToDataToSave(child_table, cfsToSave, mapKey, key.toString(), this_rowKey);
											addToDataToSave(child_table, cfsToSave, this_rowKey, inside_mapKey, val);
										}
									}
								}
								else {
									addToDataToSave(child_table, cfsToSave, mapKey, (String) key, (String) map.get(key));
								}
							}
						}

						CassandraColumnFamilyWrapper child_table_cf = factory.getCassandraColumnFamilyWrapper(child_table);

						if (cc.isLazyLoaded(prop)) {
							if (map == null) {
								if (cc.isMapOfMaps(prop))
									map = new ApolloMapImpl<Map<String, String>>(factory, idValue, mapKey, child_table_cf, prop, null, true, null);
								else
									map = new ApolloMapImpl<String>(factory, idValue, mapKey, child_table_cf, prop, null, false, null);
							}
							else {
								if (cc.isMapOfMaps(prop))
									map = new ApolloMapImpl<Map<String, String>>(factory, idValue, mapKey, child_table_cf, prop, (Map<String, Map<String, String>>) map, true, null);
								else
									map = new ApolloMapImpl<String>(factory, idValue, mapKey, child_table_cf, prop, (Map<String, String>) map, false, null);
							}

							cc.setPropertyMethodValue(object, prop, map);
						}
					}
				}
				else if (returnType == List.class) {
					List<?> list = (List<?>) cc.getPropertyMethodValue(object, prop);
					
					if (!(list instanceof ApolloListImpl)) {
						String _class = propConfig.get(ATTR_VALUE_TYPE);

						Class<?> value_type = _class == null ? String.class : Class.forName(_class);

						boolean isNative = Util.isNativelySupported(value_type);

						String listKey = cc.getListKey(prop, idValue);
						
						String table = propConfig.get(ATTR_TABLE);
						
						if (table == null) table = cc.cfName;
						
						CassandraColumnFamilyWrapper child_cf = factory.getCassandraColumnFamilyWrapper(table);
						
						addToDataToSave(cc.cfName, cfsToSave, idValue, column, listKey);
						
						addToNoAutoDelete(no_auto_delete_list, propConfig, cc.cfName, listKey);
						
						if (list != null) {
							for (Object object2 : list) {
								if (!value_type.isAssignableFrom(object2.getClass()))
									throw new IllegalArgumentException(object2.getClass() + " is not assignable to " + value_type);
								
								String colKey = null;
								
								if (isNative) {
									colKey = object2.toString();
								}
								else {
									ClassConfig cc2 = getClassConfig(value_type, true);
									
									colKey = (String) cc2.getIdValue(object2);
									
									if (colKey == null)
										throw new IllegalArgumentException("An instance of the class " + value_type + " has not been saved.");
								}
								
								addToDataToSave(child_cf.getColumnFamilyName(), cfsToSave, listKey, colKey, "");
							}
						}
						
						boolean lazy_loaded = cc.isLazyLoaded(prop);
						
						if (lazy_loaded) {
							list = new ApolloListImpl<T>(this, prop, child_cf, cc, listKey, "", "", MAX_COLUMN_PER_PAGE);
							
							cc.setPropertyMethodValue(object, prop, list);
						}
					}
				}
				else if (returnType == Set.class) {
					Set<?> set = (Set<?>) cc.getPropertyMethodValue(object, prop);
					
					if (!(set instanceof ApolloSetImpl<?>)) {
						Class<?> generic_type = cc.getMethodParameterizedType(prop);
						
						boolean isNative = Util.isNativelySupported(generic_type); 
						
						String _class = propConfig.get(ATTR_CLASS);

						Class<?> clazz = isNative ? generic_type : Class.forName(_class);
						
						ClassConfig cc2 = isNative ? null : classToClassConfig.get(clazz);
						
						String child_cf_rowKey = cc.getSetKey(prop, idValue);
						
						cf.insertColumn(idValue, column, child_cf_rowKey);
						
						value = set != null && set.size() > 0 ? child_cf_rowKey : null;
						
						String table = propConfig.get(ATTR_TABLE);
						
						table = table != null ? table : cc.getColumnFamily();
						
						if (logger.isDebugEnabled())
							logger.debug("set: " + set);
						
						if (set != null) {
							for (Object object2 : set) {
								
								Object setClass_idValue = cc2 != null ? cc2.getIdValue(object2) : object2.toString();
								
								if (logger.isDebugEnabled())
									logger.debug("setClass_idValue: " + setClass_idValue);
								
								addToNoAutoDelete(no_auto_delete_list, propConfig, table, child_cf_rowKey);
									
								if (setClass_idValue != null) {
									if (cfsToSave == null)
										cfsToSave = new LinkedHashMap<String, Map<String,Map<String,Serializable>>>();
									
									Map<String, Map<String, Serializable>> rowsToSave = cfsToSave.get(table);
									
									if (rowsToSave == null) {
										rowsToSave = new LinkedHashMap<String, Map<String, Serializable>>();
										cfsToSave.put(table, rowsToSave);
									}
									
									Map<String, Serializable> cols = rowsToSave.get(child_cf_rowKey);
									
									if (cols == null) {
										cols = new LinkedHashMap<String, Serializable>();
										rowsToSave.put(child_cf_rowKey, cols);
									}
									
									if (setClass_idValue != null)
										cols.put(setClass_idValue.toString(), "");
									
									if (logger.isDebugEnabled())
										logger.debug("rowsToSave: " + rowsToSave);
								}
							}
						}
					}
				}
				else {
					//String _class = propConfig.get(ATTR_CLASS);
					
					//if (logger.isDebugEnabled())
					//	logger.debug("Checking if class belongs to any of the mapped classes: " + _class);
					
					//if (_class != null) {
						Class clazz = cc.getPropertyType(prop);

						ClassConfig cc2 = classToClassConfig.get(clazz);
						
						if (cc2 != null) {
							Object _prop = cc.getPropertyMethodValue(object, prop);
							
							boolean notReallyNull = _prop != null;
							
							Object o = _prop == null ? null : cc2.getIdValue(_prop);
							
							value = o == null ? null : o.toString();
							
							if (value == null && notReallyNull)
								checkForNull = false;
						}
					//}
				}
				
				if (logger.isDebugEnabled())
					logger.debug("checkForNull: " + checkForNull + " value: " + value + " value_class: " + (value != null ? value.getClass() : "null") + " prop: " + prop + " propConfig: " + propConfig);
				
				if (checkForNull && value == null && not_null)
					throw new IllegalArgumentException("null parameter (or zero-sized collection) detected for property '" + prop + "'");
				
				if (cfsToSave == null)
					cfsToSave = new LinkedHashMap<String, Map<String,Map<String,Serializable>>>();
				
				Map<String, Map<String, Serializable>> rowsToSave = cfsToSave.get(cc.getColumnFamily());
				
				if (rowsToSave == null) {
					rowsToSave = new LinkedHashMap<String, Map<String, Serializable>>();
					cfsToSave.put(cc.getColumnFamily(), rowsToSave);
				}
				
				Map<String, Serializable> cols = rowsToSave.get(idValue);
				
				if (cols == null) {
					cols = new LinkedHashMap<String, Serializable>();
					rowsToSave.put(idValue, cols);
				}
				
				if (value != null)
					cols.put(column, value);
			}
			
			if (logger.isDebugEnabled())
				logger.debug("*********************  All the requirements to save records seem all pretty ok  *********************");
			
			if (cfsToSave != null) {
				for (String columnFamily : cfsToSave.keySet()) {
					
					Map<String, Map<String, Serializable>> rowsToSave = cfsToSave.get(columnFamily);
					
					CassandraColumnFamilyWrapper _cf = factory.getCassandraColumnFamilyWrapper(columnFamily);
					
					if (rowsToSave != null) {
						for (String rowKey : rowsToSave.keySet()) {
							String log_prefix = columnFamily + "[" + rowKey + "]";
							
							if (logger.isDebugEnabled())
								logger.debug("Saving rowKey: " + rowKey);
							
							Map<String, Serializable> cols = rowsToSave.get(rowKey);
							
							boolean delete_collection_type = true;
							
							List<String> _no_auto_delete_list = no_auto_delete_list.get(columnFamily);

							if (logger.isDebugEnabled())
								logger.debug(log_prefix + " _no_auto_delete_list: " + _no_auto_delete_list);

							if (_no_auto_delete_list != null)
								delete_collection_type = !_no_auto_delete_list.contains(rowKey);
							
							if (delete_collection_type && 
									(rowKey.startsWith(SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_KEY_COLLECTION_INDEX) 
									|| rowKey.startsWith(SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_KEY_COLLECTION_INDEX))) {
								if (logger.isDebugEnabled())
									logger.debug("[CF:"+columnFamily+"] Auto-deleting row with key: " + rowKey);
								
								_cf.deleteRow(rowKey);
							}
							else
								if (logger.isDebugEnabled())
									logger.debug(log_prefix + " Not auto-deleting collection type with id: " + rowKey);
							
							for (String col : cols.keySet()) {
								_cf.insertColumn(rowKey, col, cols.get(col));
							}

							Serializable _rstat = _cf.getColumnValue(idValue, cc.getRStatColumnName());
							
							int rstat = 0;

							if (_rstat != null && _rstat.getClass() == String.class)
								try {
									rstat = Integer.parseInt((String) _rstat);
								} catch (Exception e) {
								}
							else if (_rstat != null && _rstat.getClass() == int.class)
								rstat = (Integer) _rstat;
								
							_cf.insertColumn(idValue, cc.getRStatColumnName(), rstat);
						}
					}
				}
			}
			
			if (logger.isDebugEnabled()) logger.debug("*********************  FINISHING: Saving class "+object.getClass()+" object " + object + " **************************");
			return object;
		} catch (Exception e) {
			e.printStackTrace();
			
			throw new ApolloException(e);
		}
	}

	private boolean addToNoAutoDelete(
			Map<String, List<String>> no_auto_delete_list,
			Map<String, String> propConfig, String column_family, String rowKey) {
		
		if (!Util.getBooleanValue(propConfig.get(ATTR_CASCADE_DELETE), true)) {
			List<String> _no_auto_delete_list = no_auto_delete_list.get(column_family);
			
			if (_no_auto_delete_list == null) {
				_no_auto_delete_list = new ArrayList<String>();
				
				no_auto_delete_list.put(column_family, _no_auto_delete_list);
			}
			
			if (!_no_auto_delete_list.contains(rowKey)) {
				_no_auto_delete_list.add(rowKey);
				
				return true;
			}
		}
		
		return false;
	}

	private void addToDataToSave(String cfName,
			Map<String, Map<String, Map<String, Serializable>>> cfsToSave,
			String rowKey, String column, String value) {
		Map<String, Map<String, Serializable>> rowsToSave = cfsToSave.get(cfName);
		
		if (rowsToSave == null) {
			rowsToSave = new LinkedHashMap<String, Map<String,Serializable>>();
			
			cfsToSave.put(cfName, rowsToSave);
		}
		
		Map<String, Serializable> colsToSave = rowsToSave.get(rowKey);
		
		if (colsToSave == null) {
			colsToSave = new LinkedHashMap<String, Serializable>();
			
			rowsToSave.put(rowKey, colsToSave);
		}
		
		colsToSave.put(column, value);
	}

	public SessionFactory getSessionFactory() {
		return factory;
	}
	
	String getCacheKey(Class<?> c, String idValue) {
		return c + "@" + idValue;
	}
	
	Object getObjectFromCache(Class<?> c, String idValue) {
		Cache cache = factory.getCacheManager().getCache(CACHE_NAME);
		
		Object o = null;
		
		String key = getCacheKey(c, idValue);
		
		if (cache != null) {
			Element e = cache.get(key);
			
			if (e != null)
				o = e.getObjectValue();
		}
		
		if (o == null)
			if (logger.isDebugEnabled()) logger.debug("There's nothing in the cache with key " + key);
		else
			if (logger.isDebugEnabled()) logger.debug("Object with key " + key + " is found in the cache.");
		
		return o;
	}
	
	Object getObjectFromCache(Object o, String idValue) {
		Object ret = getObjectFromCache(o.getClass(), idValue);
		
		ret = ret == null ? o : ret;
		
		return ret;
	}
	
	void setObjectToCache(Object o, String idValue) {
		Cache cache = factory.getCacheManager().getCache(CACHE_NAME);
		
		if (cache != null) {
			String key = getCacheKey(o.getClass(), idValue);
			Element e = cache.get(key);
			
			if (e != null)
				cache.remove(key);
			
			e = new Element(key, o);
			
			cache.put(e);
			
			if (logger.isDebugEnabled()) logger.debug("Object is pushed to the cache with key " + key);
		}
	}

	public void delete(Object object) throws ApolloException {
		try {
			ClassConfig cc = getClassConfig(object);
			
			CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(cc.cfName);
			
			Object idValue = cc.getIdValue(object);
			
			cf.deleteRow(idValue.toString());
			
			removeObjectFromCache(cc.clazz, idValue);
			
			for (String prop : cc.getMethods()) {
				Class<?> propType = cc.getPropertyType(prop);
				
				String cascade = cc.getMethodConfig(prop).get(ATTR_CASCADE);
				
				String child_table = null;
				String child_table_row_key = null;
				
				// if (logger.isDebugEnabled()) logger.debug("'" + prop + "' : " + propType);
				
				if (propType == Map.class) {
					child_table = cc.getMethodConfig(prop).get(ATTR_TABLE);
					child_table_row_key = idValue.toString();
					
					if (logger.isDebugEnabled()) logger.debug("property '" + prop + "' is of type " + Map.class);
					
					if (child_table != null) {
						CassandraColumnFamilyWrapper linkTable = factory.getCassandraColumnFamilyWrapper(child_table);
						
						String child_table_key_suffix = cc.getMethodConfig(prop).get(ATTR_CHILD_TABLE_KEY_SUFFIX);
						
						child_table_row_key = idValue.toString() + (child_table_key_suffix != null ? child_table_key_suffix : null);
						
						linkTable.deleteRow(child_table_row_key);
					}
				}
				else if (propType == Set.class 
						&& (cascade != null && (cascade.equalsIgnoreCase("all") 
								|| cascade.equalsIgnoreCase("delete")))) {
					child_table = cc.getMethodConfig(prop).get(ATTR_TABLE);
					child_table_row_key = idValue.toString();
					
					CassandraColumnFamilyWrapper linkTable = factory.getCassandraColumnFamilyWrapper(child_table);
					
					linkTable.deleteRow(child_table_row_key);
					
					Class<?> elementClass = Class.forName(cc.getMethodConfig(prop).get(ATTR_CLASS));
					
					ClassConfig cc2 = classToClassConfig.get(elementClass);

					if (cc2 != null) {
						Set<?> set = (Set<?>) cc.getPropertyMethodValue(object, prop);

						if (set != null && set.size() > 0) {
							for (Object object2 : set) {
								Object setIdValue = cc2.getIdValue(object2);

								if (setIdValue != null) {
									CassandraColumnFamilyWrapper setCF = factory.getCassandraColumnFamilyWrapper(cc2.cfName);

									setCF.deleteRow(setIdValue.toString());
								}
							}
						}
					}
				}
			}
		}
		catch (Exception e) {
			throw new ApolloException(e);
		}
	}
	
    private void removeObjectFromCache(Class<?> clazz, Object idValue) {
    	String key = clazz + "@" + idValue;
    	
    	Cache cache = factory.getCacheManager().getCache(CACHE_NAME);
    	
    	if (cache != null) {
    		Element e = cache.get(key);
    		
    		if (e != null) {
    			cache.remove(key);
    			
    			if (logger.isDebugEnabled()) logger.debug("An object with key " + key + " has been removed from cache.");
    		}
    		else {
    			if (logger.isDebugEnabled()) logger.debug("There no object with key " + key + " in the cache and therefore there's nothing to remove");
    		}
    	}
	}
    
    private void removeObjectFromCache(Object object, Object idValue) {
    	removeObjectFromCache(object.getClass(), idValue);
    }

	<T> T createProxy(String idValue, T obj, ClassConfig classConfig) {
        Enhancer e = new Enhancer();
 
        e.setSuperclass(obj.getClass());
        e.setCallback(new MyInterceptor(idValue, obj, factory, classConfig));
        T proxifiedObj = (T) e.create();
        
        return proxifiedObj;
    }

	public void refresh(Object object) throws ApolloException {
		if (object == null)
			throw new NullPointerException();
		
		try {
			ClassConfig cc = getClassConfig(object);
			
			String idValue = (String) cc.getIdValue(object);
			
			Object objTmp = find(cc.clazz, idValue);
			
			if (objTmp != null) {
				for (String prop : cc.getMethods()) {
					Object val = cc.getPropertyMethodValue(objTmp, prop);
					
					cc.setPropertyMethodValue(object, prop, val);
				}
			}
		} catch (Exception e) {
			throw new ApolloException(e);
		} 
	}

	public void reconnect(Object object) throws ApolloException {
		try {
			ClassConfig cc = getClassConfig(object);
			
			refresh(object);
		} catch (Exception e) {
			throw new ApolloException(e);
		} 
	}

	public void evict(Object object) throws ApolloException {
		try {
			ClassConfig cc = getClassConfig(object);
			
			String idValue = (String) cc.getIdValue(object);
		} catch (Exception e) {
			throw new ApolloException(e);
		} 
	}

	public void refresh(Object object, String prop) throws ApolloException {
		try {
			ClassConfig cc = getClassConfig(object);
			
			Map<String, String> method_config = cc.getMethodConfig(prop);
			
			if (method_config == null)
				throw new IllegalArgumentException("The property '" + prop + "' does not exist for " + cc.clazz); 
			
			Class<?> methodType = cc.getPropertyType(prop);
			
			String idValue = (String) cc.getIdValue(object);
			
			if (methodType == Integer.TYPE
					|| methodType == Long.TYPE
					|| methodType == Double.TYPE
					|| methodType == Short.TYPE
					|| methodType == Byte.TYPE
					|| methodType == Boolean.TYPE
					|| methodType == String.class
					|| methodType == Timestamp.class
					) {
				CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(cc.cfName);
				
				String val = (String) cf.getColumnValue(idValue, method_config.get(ATTR_COLUMN));
				
				setPropertyMethodValue(object, val, prop, cc);
			}
			else if (methodType == Map.class) {
				String child_table = method_config.get(ATTR_TABLE);
				
				if (child_table != null) {
					String child_table_key_suffix = method_config.get(ATTR_CHILD_TABLE_KEY_SUFFIX);
					
					String child_table_key = idValue;
					
					if (child_table_key_suffix != null)
						child_table_key += child_table_key_suffix;
					
					CassandraColumnFamilyWrapper child_table_cf = factory.getCassandraColumnFamilyWrapper(child_table);
					
					Map<String, Map<String, Serializable>> rows = child_table_cf.getColumnsAsMap(child_table_key, "", "", "", 100, 1);
					
					if (rows != null) {
						Map<String, Serializable> cols = rows.get(child_table_key);

						if (cols != null)
							setPropertyMethodValue(object, (Serializable) cols, prop, cc);
					}
				}
			}
		} catch (Exception e) {
			throw new ApolloException(e);
		} 
	}

	public <T> Criteria<T> createCriteria(Class<T> clazz) throws ApolloException {
		return new CriteriaImpl<T>(this, clazz);
	}
	
	ClassConfig getClassConfig(Class<?> clazz, boolean throwOnNotFound) {
		ClassConfig cc = classToClassConfig.get(clazz);
		
		if (cc == null && Enhancer.isEnhanced(clazz))
			cc = classToClassConfig.get(clazz.getSuperclass());
		
		if (cc == null && throwOnNotFound)
			throw new IllegalArgumentException("No mapping defined for class '" + clazz + "'");
		
		return cc;
	}

	ClassConfig getClassConfig(Object object) {
		if (object == null)
			throw new NullPointerException();
		
		return getClassConfig(object.getClass(), true);
	}
	
	public void truncate(Class<?> clazz) throws ApolloException {
		ClassConfig cc = getClassConfig(clazz, true);
		
		CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(cc.cfName);
		
		cf.truncate();
	}
	
	public <T> List<Serializable> getKeyList(Class<T> clazz, String startKey) throws Exception {
		ClassConfig cc = getClassConfig(clazz, true);
		
		List<Serializable> ret = null;
		
		List<T> list = createCriteria(clazz).list();
		
		if (list != null && list.size() > 0) {
			if (ret == null)
				ret = new ArrayList<Serializable>();
			
			for (T datasource : list) {
				ret.add(cc.getIdValue(datasource));
			}
		}
		
		return ret;
	}

	public void flush() {
	}

	public void close() {
	}
}
