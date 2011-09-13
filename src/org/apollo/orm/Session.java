package org.apollo.orm;

import java.io.Serializable;
import java.util.List;

public interface Session {
	SessionFactory getSessionFactory();
	
	<T> T get(Class<T> clazz, Serializable id) throws ApolloException;
	
	<T> T find(Class<T> clazz, Serializable id) throws ApolloException;
	
	<T> List<T> list(Class<T> clazz) throws ApolloException;
	
	<T> Criteria<T> createCriteria(Class<T> clazz) throws ApolloException;
	
	<T> T save(T object) throws ApolloException;  
	
	void delete(Object object) throws ApolloException;
	
	void refresh(Object object) throws ApolloException;
	
	void refresh(Object object, String prop) throws ApolloException;
	
	void reconnect(Object object) throws ApolloException;
	
	void evict(Object object) throws ApolloException;
	
	void truncate(Class<?> clazz) throws ApolloException;
	
	void flush();
	
	<T> List<Serializable> getKeyList(Class<T> clazz, String startKey) throws Exception;
}
