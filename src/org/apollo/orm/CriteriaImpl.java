package org.apollo.orm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import org.apache.log4j.Logger;

public class CriteriaImpl<T> implements Criteria<T> {
	private static Logger logger = Logger.getLogger(CriteriaImpl.class);
	
	private SessionImpl session;
	
	private List<Order> orders;
	
	private List<Expression> criterias;

	private int maxResults;

	private Class<T> clazz;

	private List<T> ret;

	public CriteriaImpl(SessionImpl session, Class<T> clazz) {
		this.session = session;
		this.clazz = clazz;
	}

	synchronized public List<T> list() throws ApolloException {
		if (ret != null)
			return ret;
		
		ClassConfig cc = session.getClassConfigUsingClass(clazz);
		
		CassandraColumnFamilyWrapper cf = session.getColumnFamilyUsingClassConfig(cc);
		
		List<T> ret = new ArrayList<T>();
		
		try {
			if (criterias == null) {
				List<String> keys = session.getKeyList(cf, "");

				if (keys != null && keys.size() > 0) {
					for (String key : keys) {
						T t = session.find(clazz, key);

						if (t != null)
							ret.add(t);
					}
				}
			}
			else {
				add(Expression.eq("__rstat__", "0"));
				
				Map<String, Map<String, String>> rows = cf.findColumnWithValue(criterias, session, cc, cc.getColumnsAsList());
				
				if (rows != null && rows.size() > 0) {
					List<String> orderedKeys = null;
					
					if (orders != null && orders.size() > 0) {
						orderedKeys = getOrderedKeys(orders, rows);
					}
					else {
						orderedKeys = new ArrayList<String>(rows.keySet());
					}
					
					if (logger.isDebugEnabled())
						logger.debug("Ordered Keys : " + orderedKeys);
					
					for (String rowKey : orderedKeys) {
						Map<String, String> cols = rows.get(rowKey);
						
						Object object = session.colsToObject(rowKey, cols, cc, null);
						
						ret.add((T) object);
					}
				}
			}
			
			this.ret = ret;
			
			return ret;
		} catch (Exception e) {
			throw new ApolloException(e);
		}
	}

	static List<String> getOrderedKeys(List<Order> orders, Map<String, Map<String, String>> rows) {
		List<String> orderedKeys = null;
		
		Map<String, Object> tmp = new TreeMap<String, Object>();
		
		int order_col_count = orders.size();
		
		if (logger.isDebugEnabled())
			logger.debug("order_col_count: " + order_col_count);
		
		for (String rowKey : rows.keySet()) {
			Map<String, String> cols = rows.get(rowKey);
			
			if (logger.isDebugEnabled())
				logger.debug("cols: " + cols);
			
			Map<String, Object> tmp2 = tmp;
			
			int ordersc = 0;
			
			for (Order order : orders) {
				String value = cols.get(order.getColumn());
				
				Object object = tmp2.get(value);
				
				if (object == null) {
					if (ordersc < order_col_count - 1) {
						TreeMap<String, Object> treeMap = new TreeMap<String, Object>();
						
						tmp2.put(value, treeMap);
						
						tmp2 = treeMap;
						
						if (logger.isDebugEnabled())
							logger.debug("Created a new instance of Map for value (or rather key) '" + value + "' map: " + tmp2.hashCode());
					}
					else {
						tmp2.put(value, rowKey);
						
						if (logger.isDebugEnabled())
							logger.debug("adding key: '" + value + "' value: '" + rowKey + "' map: " + tmp2.hashCode());
					}
				}
				else {
					if (ordersc < order_col_count - 1) {
						tmp2 = (Map<String, Object>) object;
					}
					else {
						tmp2.put(value, rowKey);
						
						if (logger.isDebugEnabled())
							logger.debug("adding key: '" + value + "' value: '" + rowKey + "' map: " + tmp2.hashCode());
					}
				}
				
				ordersc++;
			}
		}
		
		int ordersc = 0;
		Stack<Iterator<String>> keyStack = new Stack<Iterator<String>>();
		Stack<Map<String, Object>> mapStack = new Stack<Map<String,Object>>();
		
		keyStack.push(tmp.keySet().iterator());
		mapStack.push(tmp);
		
		Iterator<String> it = null;
		Map<String, Object> map = null;
		
		int order_depth = 0;
		
		Order order = orders.get(order_depth);
		
		do {
			it = keyStack.peek();
			map = mapStack.peek();
			
			String key = it.next();
			
			if (logger.isDebugEnabled())
				logger.debug("inspecting key: '" + key + "' map: " + map.hashCode());
			
			Object o = map.get(key);
			
			if (o instanceof Map) {
				keyStack.push(((Map) o).keySet().iterator());
				mapStack.push((Map<String, Object>) o);
				continue;
			}
			else {
				if (orderedKeys == null)
					orderedKeys = new ArrayList<String>();
				
				if (!orderedKeys.contains(o))
					if (order.getOrder().equals(Order.ASC))
						orderedKeys.add((String) o);
					else
						orderedKeys.add(0, (String) o);
			}
			
			if (!it.hasNext()) {
				keyStack.pop();
				mapStack.pop();
			}
		} while (it.hasNext());
		
		return orderedKeys;
	}

	synchronized public Criteria<T> add(Expression expr) {
		if (criterias == null)
			criterias = new ArrayList<Expression>();
		
		criterias.add(expr);
		
		return this;
	}

	public void setMaxResults(int maxResults) {
		this.maxResults = maxResults;
	}

	public void addOrder(Order order) {
		if (orders == null)
			orders = new ArrayList<Order>();
		
		orders.add(order);
	}
}
