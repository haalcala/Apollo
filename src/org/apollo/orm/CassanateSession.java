package org.apollo.orm;

import java.io.Serializable;
import java.util.List;

public interface CassanateSession {
	SessionFactory getSessionFactory();
	
	<T> T get(Class<T> clazz, Serializable id) throws CassanateException;
	
	<T> T find(Class<T> clazz, Serializable id) throws CassanateException;
	
	<T> List<T> list(Class<T> clazz) throws CassanateException;
	
	<T> Criteria<T> createCriteria(Class<T> clazz) throws CassanateException;
	
	Object save(Object object) throws CassanateException;
	
	void delete(Object object) throws CassanateException;
	
	void refresh(Object object) throws CassanateException;
	
	void refresh(Object object, String prop) throws CassanateException;
	
	void reconnect(Object object) throws CassanateException;
	
	void evict(Object object) throws CassanateException;
	
	void truncate(Class<?> clazz) throws CassanateException;
}
