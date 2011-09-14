package org.apollo.orm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class CriteriaImpl<T> implements Criteria<T> {
	private static Logger logger = Logger.getLogger(CriteriaImpl.class);
	
	private SessionImpl session;
	
	private List<Order> orders;
	
	private List<Expression> criterias;

	private int maxResults;

	private Class<T> clazz;

	public CriteriaImpl(SessionImpl session, Class<T> clazz) {
		this.session = session;
		this.clazz = clazz;
	}

	public List<T> list() throws ApolloException {
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
				Map<String, String> kvp = new LinkedHashMap<String, String>();
				
				for (Expression exp : criterias) {
					if (exp.getOperation().equals(Expression.OPERATION_EQ)) {
						kvp.put(exp.getProperty(), exp.getExpected().toString());
					}
				}
				
				kvp.put("__rstat__", "0");
				
				Map<String, Map<String, String>> rows = cf.findColumnWithValue(kvp, cc.getColumnsAsArray());
				
				if (logger.isDebugEnabled())
					logger.debug("rows: " + rows);
				
				if (rows != null && rows.size() > 0) {
					for (String rowKey : rows.keySet()) {
						Map<String, String> cols = rows.get(rowKey);
						
						if (logger.isDebugEnabled())
							logger.debug("cols: " + cols);
						
						Object object = session.colsToObject(rowKey, cols, cc, null);
						
						if (logger.isDebugEnabled())
							logger.debug("object: " + object);

						ret.add((T) object);
					}
				}
			}
			
			return ret;
		} catch (Exception e) {
			throw new ApolloException(e);
		}
	}

	public Criteria<T> add(Expression expr) {
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
