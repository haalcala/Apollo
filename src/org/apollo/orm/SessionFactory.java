package org.apollo.orm;

import java.io.InputStream;

import net.sf.ehcache.CacheManager;

public interface SessionFactory {

	void addClassConfiguration(String pathToXml) throws ApolloException;
	void addClassConfiguration(InputStream is) throws ApolloException;

	void validate() throws ApolloException;
	
	CassandraColumnFamilyWrapper getCassandraColumnFamilyWrapper(String columnFamily);
	
	CassandraKeyspaceWrapper getCassandraKeyspaceWrapper();
	
	Session getSession();
	
	CacheManager getCacheManager();
	
	void shutdown();
}
