package org.apollo.orm;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
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

	public <T> T get(Class<T> clazz, Serializable id) throws ApolloException {
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
		Map<String, Map<String, String>> rows = cf.getColumnsAsMap(id.toString(), "", "", "", 100, 1);
		
		if (rows != null && rows.size() > 0) {
			Map<String, String> cols = rows.get(id);
			
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
	
	<T> T colsToObject(String idValue, Map<String, String> cols, ClassConfig classConfig, Object inverse) throws Exception {
		T ret = (T) classConfig.clazz.newInstance();
		
		classConfig.setIdValue(ret, idValue);

		setObjectToCache(ret, idValue);
		
		for (String prop : classConfig.getMethods()) {
			ret = doPropertyInjection(idValue, cols, classConfig, ret, prop, false);
			
			if (ret == null) {
				logger.warn("Returning null coz doPropertyInjection method returned 'null' which is usually caused by missing required fields.");
				
				removeObjectFromCache(classConfig.clazz, idValue);
				return null;
			}
		}
		
		/*
		if (classConfig.proxyMethods != null && classConfig.proxyMethods.size() > 0)
			ret = createProxy(idValue, ret, classConfig);
		*/
		
		return ret;
	}

	<T> T doPropertyInjection(String idValue, Map<String, String> cols, ClassConfig classConfig, T ret, String prop, boolean force)
			throws Exception, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		Map<String, String> method_config = classConfig.getMethodConfig(prop);
		
		Class<?> type = classConfig.getPropertyType(prop);
		
		if (type == Set.class) {
			commenceSetPropertyInjection(ret, prop, classConfig);
		}
		else if (type == Map.class) {
			String table = method_config.get(ATTR_TABLE);
			//String _lazy_loaded = method_config.get(ATTR_LAZY_LOADED);
			
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
		else {
			String val = cols.get(method_config.get(ATTR_COLUMN));
			
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
		Map map = (Map) classConfig.getPropertyMethodValue(ret, prop);
		
		for (String col : cols.keySet()) {
			if (col.startsWith(prop + "-")) {
				String key = col.substring(col.indexOf('-') + 1);
				String val = cols.get(col);
				
				if (map == null) 
					map = new LinkedHashMap<String, String>();
				
				map.put(key, val);
			}
		}
		
		if (map != null && map.size() > 0)
			classConfig.setPropertyMethodValue(ret, prop, map);
	}

	private void setPropertyMethodValue(Object ret, Object val, String prop, ClassConfig classConfig)
			throws Exception {
		Class<?> paramType = classConfig.getPropertyType(prop);

		Object value = null;
		
		if (paramType == Integer.TYPE) {
			value = Integer.parseInt((String) val);
		}
		else if (paramType == Long.TYPE) {
			value = Long.parseLong((String) val);
		}
		else if (paramType == Double.TYPE) {
			value = Double.parseDouble((String) val);
		}
		else if (paramType == Float.TYPE) {
			value = Float.parseFloat((String) val);
		}
		else if (paramType == Boolean.TYPE) {
			value = Boolean.parseBoolean((String) val);
		}
		else if (paramType == Short.TYPE) {
			value = Short.parseShort((String) val);
		}
		else if (paramType == Byte.TYPE) {
			value = Byte.parseByte((String) val);
		}
		else if (paramType == String.class) {
			value = val;
		}
		else if (paramType == Timestamp.class) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

			value = new Timestamp(sdf.parse((String) val).getTime());
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
			
			value = getObjectFromCache(paramType, (String) val);
			
			if (value == null) {
				value = get(paramType, (Serializable) val);

				setObjectToCache(value, (String) val);
			}
		}

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
		
		String child_table_rowKey = cf.getColumnValue(idValue, classConfig.getMethodColumn(prop, true));
		
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
	
	public <T> T find(Class<T> clazz, Serializable id, Object inverse) throws ApolloException {
		ClassConfig cc = getClassConfigUsingClass(clazz);
		
		if (cc == null)
			throw new ApolloException("Class " + clazz + " has not been registered.");
		
		CassandraColumnFamilyWrapper cf = getColumnFamilyUsingClassConfig(cc);
		
		try {
			return getEntityUsingId(cf, id, cc, null);
		} catch (Exception e) {
			throw new ApolloException(e);
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
		
		Map<String, Map<String, String>> rows = columnFamilyWrapper.getColumnsAsMap(startKey, "", "", "", 1, maxRows, true);

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
		
		try {
			ClassConfig cc = getClassConfig(object);
			
			final CassandraColumnFamilyWrapper cf = factory.getCassandraColumnFamilyWrapper(cc.getColumnFamily());
			
			Map<String, Map<String, Map<String, String>>> cfsToSave = null;
			
			Map<String, Map<String, String>> rowsToSave = null;
			
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
				
				Object value = null;
				
				Class<?> returnType = cc.getPropertyType(prop);
				
				//if (logger.isDebugEnabled()) logger.debug("returnType: " + returnType);
				
				boolean checkForNull = true;
				
				if (returnType == Integer.TYPE
						|| returnType == Long.TYPE
						|| returnType == Double.TYPE
						|| returnType == Float.TYPE
						|| returnType == Byte.TYPE
						|| returnType == Short.TYPE
						|| returnType == Boolean.TYPE
						|| returnType == String.class) {
					Object o = cc.getPropertyMethodValue(object, prop);
					
					value = o == null ? null : o.toString();
				}
				else if (returnType == Timestamp.class) {
					Timestamp ts = (Timestamp) cc.getPropertyMethodValue(object, prop);

					value = ts != null ? Util.getString(ts) : null;
				}
				else if (returnType == Map.class) {
					// if (logger.isDebugEnabled()) logger.debug("Saving property '" + prop + "' of type " + Map.class);
					
					/*
					 * Check if there's any table specified
					 */
					String child_table = propConfig.get(ATTR_TABLE);
					
					value = Util.getMapKey(idValue, prop);
					
					Object method_value = cc.getPropertyMethodValue(object, prop);
					
					if (!(method_value instanceof ApolloMapImpl<?>)) {
						Map<String, ?> map = (Map<String, String>) cc.getPropertyMethodValue(object, prop);

						if (logger.isDebugEnabled()) logger.debug("map : " + map);

						if (map != null && map.size() > 0) {
							for (String key : map.keySet()) {
								String mapKey = Util.getMapKey(idValue, prop);

								if (rowsToSave == null) 
									rowsToSave = new LinkedHashMap<String, Map<String, String>>();

								Map<String, String> cols = rowsToSave.get(mapKey);

								if (cols == null) {
									cols = new LinkedHashMap<String, String>();
									rowsToSave.put(mapKey, cols);
								}

								cols.put(key, (String) map.get(key));
							}
						}

						CassandraColumnFamilyWrapper child_table_cf = factory.getCassandraColumnFamilyWrapper(child_table);

						if (method_value == null) {
							if (cc.isMapOfMaps(prop))
								map = new ApolloMapImpl<Map<String, String>>(factory, idValue, child_table_cf, prop, null, true, null);
							else
								map = new ApolloMapImpl<String>(factory, idValue, child_table_cf, prop, null, false, null);
						}
						else {
							if (cc.isMapOfMaps(prop))
								map = new ApolloMapImpl<Map<String, String>>(factory, idValue, child_table_cf, prop, (Map<String, Map<String, String>>) method_value, true, null);
							else
								map = new ApolloMapImpl<String>(factory, idValue, child_table_cf, prop, (Map<String, String>) method_value, false, null);
						}

						cc.setPropertyMethodValue(object, prop, map);
					}
				}
				else if (returnType == List.class) {
					List<?> list = (List<?>) cc.getPropertyMethodValue(object, prop);
					
					if (list != null)
						value = listToCSV(list);
				}
				else if (returnType == Set.class) {
					Set<?> set = (Set<?>) cc.getPropertyMethodValue(object, prop);
					
					Class<?> generic_type = cc.getMethodParameterizedType(prop);
					
					boolean isNative = Util.isNativelySupported(generic_type);
					
					Map<String, String> methodConfig = cc.getMethodConfig(prop);
					
					String _class = methodConfig.get(ATTR_CLASS);

					Class<?> clazz = isNative ? generic_type : Class.forName(_class);
					
					ClassConfig cc2 = isNative ? null : classToClassConfig.get(clazz);
					
					String child_table_key_suffix = methodConfig.get(ATTR_CHILD_TABLE_KEY_SUFFIX);
					
					String child_cf_rowKey = Util.getSetKey(idValue, prop, child_table_key_suffix);
					
					cf.insertColumn(idValue, column, child_cf_rowKey);
					
					value = Util.getSetKey(idValue, prop);
					
					String table = methodConfig.get(ATTR_TABLE);
					
					if (set != null) {
						for (Object object2 : set) {
							Object setClass_idValue = cc2 != null ? cc2.getIdValue(object2) : object2.toString();
							
							if (setClass_idValue != null) {
								if (cfsToSave == null)
									cfsToSave = new LinkedHashMap<String, Map<String,Map<String,String>>>();
								
								rowsToSave = cfsToSave.get(table);
								
								if (rowsToSave == null) {
									rowsToSave = new LinkedHashMap<String, Map<String, String>>();
									cfsToSave.put(table, rowsToSave);
								}
								
								Map<String, String> cols = rowsToSave.get(child_cf_rowKey);
								
								if (cols == null) {
									cols = new LinkedHashMap<String, String>();
									rowsToSave.put(child_cf_rowKey, cols);
								}
								
								if (setClass_idValue != null)
									cols.put(TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString(), setClass_idValue.toString());
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
					logger.debug("checkForNull: " + checkForNull + " value: " + value + " prop: " + prop + " propConfig: " + propConfig);
				
				if (checkForNull && value == null && propConfig.get(ATTR_NOT_NULL) != null 
						&& propConfig.get(ATTR_NOT_NULL).equals("true"))
					throw new IllegalArgumentException("null parameter detected for property '" + prop + "'");
				
				if (cfsToSave == null)
					cfsToSave = new LinkedHashMap<String, Map<String,Map<String,String>>>();
				
				rowsToSave = cfsToSave.get(cc.getColumnFamily());
				
				if (rowsToSave == null) {
					rowsToSave = new LinkedHashMap<String, Map<String, String>>();
					cfsToSave.put(cc.getColumnFamily(), rowsToSave);
				}
				
				Map<String, String> cols = rowsToSave.get(idValue);
				
				if (cols == null) {
					cols = new LinkedHashMap<String, String>();
					rowsToSave.put(idValue, cols);
				}
				
				if (value != null)
					cols.put(column, value.toString());
			}
			
			if (cfsToSave != null) {
				for (String columnFamily : cfsToSave.keySet()) {
					CassandraColumnFamilyWrapper _cf = factory.getCassandraColumnFamilyWrapper(columnFamily);
					
					if (rowsToSave != null) {
						for (String rowKey : rowsToSave.keySet()) {
							if (logger.isDebugEnabled())
								logger.debug("Saving rowKey: " + rowKey);
							
							Map<String, String> cols = rowsToSave.get(rowKey);
							
							if (rowKey.startsWith(SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_MAP_KEY_PREFIX) 
									|| rowKey.startsWith(SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_SET_KEY_INDEX))
								_cf.deleteRow(rowKey);
							
							for (String col : cols.keySet()) {
								_cf.insertColumn(rowKey, col, cols.get(col));
							}

							String _rstat = _cf.getColumnValue(idValue, SYS_COL_RSTAT);
							
							int rstat = 0;
							
							try {
								rstat = Integer.parseInt(_rstat);
							} catch (Exception e) {
							}
							
							_cf.insertColumn(idValue, SYS_COL_RSTAT, "" + rstat);
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
				
				String val = cf.getColumnValue(idValue, method_config.get(ATTR_COLUMN));
				
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
					
					Map<String, Map<String, String>> rows = child_table_cf.getColumnsAsMap(child_table_key, "", "", "", 100, 1);
					
					if (rows != null) {
						Map<String, String> cols = rows.get(child_table_key);

						if (cols != null)
							setPropertyMethodValue(object, cols, prop, cc);
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
