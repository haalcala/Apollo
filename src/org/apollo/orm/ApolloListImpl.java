package org.apollo.orm;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.apollo.orm.ApolloConstants.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApolloListImpl<T> implements ApolloList<T> {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
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
	
	public ApolloListImpl(Session session, String prop, CassandraColumnFamilyWrapper cf, ClassConfig classConfig, String rowKey, String startCol, String endCol, int maxCols) throws Exception {
		this.rowKey = rowKey;
		this.startCol = startCol;
		this.endCol = endCol;
		this.maxCols = maxCols;
		this.cf = cf;
		this.classConfig = classConfig;
		this.session = session;
		this.prop = prop;
		
		if (classConfig != null)
			parameter_type = classConfig.getMethodParameterizedType(prop);
		
		if (parameter_type == null)
			parameter_type = String.class;
		
		isNative = Util.isNativelySupported(parameter_type);
		
		if (logger.isDebugEnabled())
			logger.debug("ApolloSetImpl:: rowKey: " + rowKey + " startCol: " + startCol + " endCol: " + endCol 
						+ " maxCols: " + maxCols + " prop: " + prop + " cf: " + cf + (cf != null ? " " + cf.getColumnFamilyName() : ""));
	}

	public int size() {
		if (logger.isDebugEnabled())
			logger.debug("size called");
		
		return -1;
	}

	public boolean isEmpty() {
		if (logger.isDebugEnabled())
			logger.debug("isEmpty called");
		
		return false;
	}

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

	public Iterator<T> iterator() {
		if (logger.isDebugEnabled())
			logger.debug("iterator called");
		
		return new Iterator<T>() {
			Iterator<String> it = Util.getApolloColumnIterator(cf, rowKey);
			
			String currentKey;
			
			boolean consumed;

			public boolean hasNext() {
				return it.hasNext();
			}

			public T next() {
				try {
					currentKey = it.next();
					
					T ret = null;
					
					if (logger.isDebugEnabled())
						logger.debug("prop: " + prop + " c: " + parameter_type + " Util.isNativelySupported(c): " 
									+ Util.isNativelySupported(parameter_type) + " currentKey: " + currentKey);
					
					//String val = cf.getColumnValue(rowKey, currentKey);
					
					if (logger.isDebugEnabled())
						logger.debug("val: " + currentKey);
					
					if (isNative) {
						ret = (T) Util.getNativeValueFromString(parameter_type, currentKey);
					}
					else {
						String idValue = (String) cf.getColumnValue(rowKey, currentKey);
						
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

			public void remove() {
				if (currentKey == null)
					throw new NoSuchElementException();
				
				cf.deleteColumn(rowKey, currentKey);
			}
		};
	}

	public Object[] toArray() {
		if (logger.isDebugEnabled())
			logger.debug("toArray called");
		
		return null;
	}

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
				cf.insertColumn(rowKey, val, "");
			
			return false;
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1);
		}
	}
	
	void checkValidClass() {
		
	}

	public boolean remove(Object o) {
		checkValidClass();
		
		if (isNative) {
			cf.deleteColumn(rowKey, o.toString());
		}
		
		return true;
	}

	public boolean containsAll(Collection<?> c) {
		if (logger.isDebugEnabled())
			logger.debug("containsAll called");
		
		return false;
	}

	public boolean addAll(Collection<? extends T> c) {
		if (logger.isDebugEnabled())
			logger.debug("addAll called");
		
		return false;
	}

	public boolean retainAll(Collection<?> c) {
		if (logger.isDebugEnabled())
			logger.debug("retainAll called");
		
		return false;
	}

	public boolean removeAll(Collection<?> c) {
		if (logger.isDebugEnabled())
			logger.debug("removeAll called");
		
		
		for (Object object : c) {
			if (object.getClass() != parameter_type)
				throw new IllegalArgumentException("");
			
			remove(object);
		}
		
		return true;
	}

	public void clear() {
		cf.deleteRow(rowKey);
	}

	public boolean addAll(int index, Collection<? extends T> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public T get(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public T set(int index, T element) {
		// TODO Auto-generated method stub
		return null;
	}

	public void add(int index, T element) {
		// TODO Auto-generated method stub
		
	}

	public T remove(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public int indexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public ListIterator<T> listIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	public ListIterator<T> listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<T> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("hiding")
	public <T> T[] toArray(T[] a) {
		if (logger.isDebugEnabled())
			logger.debug("toArray(T{} a) called");
		
		return null;
	}
}
