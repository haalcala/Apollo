package org.apollo.orm;

import java.io.InputStream;

import net.sf.ehcache.CacheManager;

public interface SessionFactory {

	void addClassConfiguration(String pathToXml) throws CassanateException;
	void addClassConfiguration(InputStream is) throws CassanateException;

	void validate() throws CassanateException;
	
	CassandraColumnFamilyWrapper getCassandraColumnFamilyWrapper(String columnFamily);
	
	CassandraKeyspaceWrapper getCassandraKeyspaceWrapper();
	
	CassanateSession getSession();
	
	CacheManager getCacheManager();
	
	void shutdown();
}
