package org.apollo.orm;

import java.util.ArrayList;
import java.util.List;

public class CriteriaImpl<T> implements Criteria<T> {
	
	private CassanateSessionImpl session;
	
	private List<Order> orders;
	
	private List<Expression> criterias;

	private int maxResults;

	private Class<T> clazz;

	public CriteriaImpl(CassanateSessionImpl session, Class<T> clazz) {
		this.session = session;
		this.clazz = clazz;
	}

	public List<T> list() throws CassanateException {
		ClassConfig cc = session.getClassConfigUsingClass(clazz);
		
		CassandraColumnFamilyWrapper cf = session.getColumnFamilyUsingClassConfig(cc);
		
		List<T> ret = new ArrayList<T>();
		
		try {
			List<String> keys = session.getKeyList(cf, "");
			
			if (keys != null && keys.size() > 0) {
				for (String key : keys) {
					T t = session.find(clazz, key);

					if (t != null)
						ret.add(t);
				}
			}
			
			return ret;
		} catch (Exception e) {
			throw new CassanateException(e);
		}
	}

	public Criteria<T> addCriteria(Expression expr) {
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
