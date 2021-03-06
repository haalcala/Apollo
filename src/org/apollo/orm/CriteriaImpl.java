package org.apollo.orm;

import static org.apollo.orm.ApolloConstants.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CriteriaImpl<T> implements Criteria<T> {
	private static Logger logger = LoggerFactory.getLogger(CriteriaImpl.class);
	
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
						if (key.startsWith(SYS_APOLLO_SYMBOL_PREFIX))
							continue;
						
						T t = session.find(clazz, key);

						if (t != null)
							ret.add(t);
					}
				}
			}
			else {
				add(Expression.eq(cc.getRStatColumnName(), "0"));
				
				Map<String, Map<String, Serializable>> rows = cf.findColumnWithValue(criterias, session, cc, cc.getColumnsAsList());
				
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
						if (rowKey.startsWith(SYS_APOLLO_SYMBOL_PREFIX))
							continue;
						
						Map<String, Serializable> cols = rows.get(rowKey);
						
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

	static List<String> getOrderedKeys(List<Order> orders, Map<String, Map<String, Serializable>> rows) {
		List<String> orderedKeys = null;
		
		Map<Serializable, Serializable> tmp = new TreeMap<Serializable, Serializable>();
		
		int order_col_count = orders.size();
		
		if (logger.isDebugEnabled())
			logger.debug("order_col_count: " + order_col_count);
		
		for (String rowKey : rows.keySet()) {
			Map<String, Serializable> cols = rows.get(rowKey);
			
			if (logger.isDebugEnabled())
				logger.debug("cols: " + cols);
			
			Map<Serializable, Serializable> tmp2 = tmp;
			
			int ordersc = 0;
			
			for (Order order : orders) {
				Serializable value = cols.get(order.getColumn());
				
				Object object = tmp2.get(value);
				
				if (object == null) {
					if (ordersc < order_col_count - 1) {
						TreeMap<Serializable, Serializable> treeMap = new TreeMap<Serializable, Serializable>();
						
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
						tmp2 = (Map<Serializable, Serializable>) object;
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
		Stack<Iterator<Serializable>> keyStack = new Stack<Iterator<Serializable>>();
		Stack<Map<Serializable, Serializable>> mapStack = new Stack<Map<Serializable,Serializable>>();
		
		keyStack.push(tmp.keySet().iterator());
		mapStack.push(tmp);
		
		Iterator<Serializable> it = null;
		Map<Serializable, Serializable> map = null;
		
		int order_depth = 0;
		
		Order order = orders.get(order_depth);
		
		do {
			it = keyStack.peek();
			map = mapStack.peek();
			
			Serializable key = it.next();
			
			if (logger.isDebugEnabled())
				logger.debug("inspecting key: '" + key + "' map: " + map.hashCode());
			
			Object o = map.get(key);
			
			if (o instanceof Map) {
				keyStack.push(((Map) o).keySet().iterator());
				mapStack.push((Map<Serializable, Serializable>) o);
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
