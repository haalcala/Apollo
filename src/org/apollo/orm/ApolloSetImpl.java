package org.apollo.orm;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.apache.log4j.Logger;
import org.apollo.orm.ApolloConstants.Util;

public class ApolloSetImpl<T> implements ApolloSet<T> {
	private Logger logger = Logger.getLogger(getClass());
	
	private String rowKey;
	
	private String startCol;
	
	private String endCol;
	
	private int maxCols;
	
	private CassandraColumnFamilyWrapper cf;
	
	private ClassConfig classConfig;

	private Session session;

	private String prop;

	private Class<?> parameter_type;

	private boolean isNative;
	
	public ApolloSetImpl(Session session, String prop, CassandraColumnFamilyWrapper cf, ClassConfig classConfig, String rowKey, String startCol, String endCol, int maxCols) throws Exception {
		this.rowKey = rowKey;
		this.startCol = startCol;
		this.endCol = endCol;
		this.maxCols = maxCols;
		this.cf = cf;
		this.classConfig = classConfig;
		this.session = session;
		this.prop = prop;
		
		parameter_type = classConfig.getMethodParameterizedType(prop);
		
		isNative = Util.isNativelySupported(parameter_type);
		
		if (logger.isDebugEnabled())
			logger.debug("ApolloSetImpl:: rowKey: " + rowKey + " startCol: " + startCol + " endCol: " + endCol + " maxCols: " + maxCols + " prop: " + prop);
	}

	@Override
	public int size() {
		return -1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public boolean contains(Object o) {
		if (o == null)
			throw new NullPointerException();
		
		if (o.getClass() != classConfig.clazz)
			throw new IllegalArgumentException("The parameter's class '" + o.getClass() + " doesn't match expected class '" + classConfig.clazz + "'");
		
		try {
			String val = ApolloConstants.Util.getObjectValue(o, (SessionImpl) session);
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
		
		return true;
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			Iterator<String> it = Util.getApolloColumnIterator(cf, rowKey);
			
			String currentKey;

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public T next() {
				try {
					currentKey = it.next();
					
					T ret = null;
					
					if (logger.isDebugEnabled())
						logger.debug("prop: " + prop + " c: " + parameter_type + " Util.isNativelySupported(c): " 
									+ Util.isNativelySupported(parameter_type) + " currentKey: " + currentKey);
					
					String val = cf.getColumnValue(rowKey, currentKey);
					
					if (logger.isDebugEnabled())
						logger.debug("val: " + val);
					
					if (isNative) {
						ret = (T) val;
					}
					else if (parameter_type == Timestamp.class) {
						ret = (T) Util.getTimestamp(val);
					}
					else {
						String idValue = cf.getColumnValue(rowKey, currentKey);
						
						ret = (T) session.find(parameter_type, idValue);
					}
					
					if (logger.isDebugEnabled())
						logger.debug("ret : " + ret);
					
					return ret;
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException();
				}
			}

			@Override
			public void remove() {
				if (currentKey == null)
					throw new NoSuchElementException();
				
				cf.deleteColumn(rowKey, currentKey);
			}
		};
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean add(T e) {
		try {
			if (e == null)
				throw new NullPointerException();
			
			if (e.getClass() != parameter_type)
				throw new IllegalArgumentException();
			
			String val = null;
			
			if (isNative) {
				val = e.toString();
			}
			else if (parameter_type == Timestamp.class) {
				val = Util.getString((Timestamp) e);
			}
			else {
				ClassConfig cc2 = ((SessionImpl) session).getClassConfig(parameter_type);
				
				val = (String) cc2.getIdValue(e);
			}
			
			if (val != null)
				cf.insertColumn(rowKey, TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString(), val);
			
			return false;
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1);
		}
	}
	
	void checkValidClass() {
		
	}

	@Override
	public boolean remove(Object o) {
		checkValidClass();
		
		if (isNative) {
			Iterator<String> it = Util.getApolloColumnIterator(cf, rowKey);
			
			for (; it.hasNext(); ) {
				String key = it.next();
				
				String val = cf.getColumnValue(rowKey, key);
				
				if (val != null && val.equals(o)) {
					cf.deleteColumn(rowKey, key);
					break;
				}
			}
		}
		
		return true;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		
		for (Object object : c) {
			if (object.getClass() != parameter_type)
				throw new IllegalArgumentException("");
			
			remove(object);
		}
		
		return true;
	}

	@Override
	public void clear() {
		cf.deleteRow(rowKey);
	}
}
