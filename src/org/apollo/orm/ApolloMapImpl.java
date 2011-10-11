package org.apollo.orm;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.apache.log4j.Logger;
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

	@Autowired
	public ApolloMapImpl(SessionFactory factory, String rowKey, CassandraColumnFamilyWrapper cf, 
			String prop, Map<String, T> initialData, boolean mapOfMaps, ClassConfig classConfig) throws SecurityException, NoSuchMethodException {
		this.factory = factory;
		this.cf = cf;
		this.rowKey = rowKey;
		this.prop = prop;
		this.mapOfMaps = mapOfMaps;
		
		if (logger.isDebugEnabled())
			logger.debug("CassanateHashMap() rowKey: " + rowKey);
		
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
			
			if (!mapOfMaps) {
				ret = (T) cf.getColumnValue(rowKey, key.toString());
			}
			else {
				try {
					mapKey = cf.getColumnValue(getKeyIndexRowKey(rowKey), key.toString());
					
					if (logger.isDebugEnabled())
						logger.debug("mapKey: " + mapKey + " key: '" + key + "' rowKey: " + rowKey);
					
					if (mapKey == null) {
						mapKey = getNewSubKey(rowKey, key.toString());
						
						if (logger.isDebugEnabled())
							logger.debug("Creating new mapKey: " + mapKey + " rowKey: " + rowKey);
						
						cf.insertColumn(getKeyIndexRowKey(rowKey), key.toString(), mapKey);
					}
					
					ret = (T) new ApolloMapImpl<String>(factory, mapKey, cf, prop, null, false, null);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			
			if (ret != null)
				map.put(key.toString(), ret);
		}
		
		return ret;
	}
	
	public String getKeyIndexRowKey(String rowKey) {
		return rowKey + "-key-index";
	}
	
	public String getNewSubKey(String rowKey, String key) {
		return rowKey +"["+key.toString()+"][" + TimeUUIDUtils.getUniqueTimeUUIDinMillis() + "]";
	}

	public boolean isEmpty() {
		return getMap().isEmpty();
	}

	public Set<String> keySet() {
		return new Set<String>() {
			
			Map<String, String> cols;
			
			@Override
			public <T> T[] toArray(T[] a) {
				
				
				
				return a;
			}
			
			@Override
			public Object[] toArray() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public int size() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public boolean retainAll(Collection<?> c) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean removeAll(Collection<?> c) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean remove(Object o) {
				if (o != null)
					cf.deleteColumn(rowKey, o.toString());
				
				return o != null;
			}
			
			@Override
			public Iterator<String> iterator() {
				return new Iterator<String>() {
					
					String curCol;
					
					Iterator<String> col_it;
					
					int coli;
					
					@Override
					public void remove() {
						if (curCol == null)
							throw new NoSuchElementException();
						
						cf.deleteColumn(rowKey, curCol);
					}
					
					@Override
					public String next() {
						if (col_it != null) { 
							curCol = col_it.next();
							coli++;
							return curCol;
						}
						
						throw new NoSuchElementException();
					}
					
					void loadNewPage() {
						if (coli == 0 || coli > ApolloConstants.MAX_COLUMN_PER_PAGE) {
							Map<String, Map<String, String>> rows = cf.getColumnsAsMap(rowKey, "", curCol, "", ApolloConstants.MAX_COLUMN_PER_PAGE + 1, 1);
							
							if (rows != null && rows.size() > 0) {
								cols = rows.get(rowKey);
								
								col_it = cols.keySet().iterator();
								
								coli = 0;
							}
						}
					}
					
					@Override
					public boolean hasNext() {
						loadNewPage();
						
						if (col_it != null)
							col_it.hasNext();
						
						return false;
					}
				};
			}
			
			@Override
			public boolean isEmpty() {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean containsAll(Collection<?> c) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean contains(Object o) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public void clear() {
				
			}
			
			@Override
			public boolean addAll(Collection<? extends String> c) {
				
				for (String str : c) {
					cf.insertColumn(rowKey, str, "");
				}
				
				return true;
			}
			
			@Override
			public boolean add(String str) {
				cf.insertColumn(rowKey, str, "");
				
				return true;
			}
		};
	}

	public T put(String key, T value) {
		T ret = null;
		
		if (!mapOfMaps) {
			cf.insertColumn(rowKey, key, value.toString());
		}
		else {
			T cols = (T) getMap().get(key.toString());
			
			if (cols == null) {
				String mapKey = getNewSubKey(rowKey, key);
				
				try {
					cols = (T) new ApolloMapImpl<String>(factory, mapKey, cf, prop, (Map<String, String>) value, false, null);

					value = cols;

					cf.insertColumn(getKeyIndexRowKey(rowKey), key, mapKey);
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
