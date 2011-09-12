package org.apollo.orm;

import java.util.List;

public interface Criteria<T> {
	List<T> list() throws ApolloException;
	
	<T> Criteria<T> addCriteria(Expression expr);
	
	void setMaxResults(int maxResults);
	
	void addOrder(Order order);
}
