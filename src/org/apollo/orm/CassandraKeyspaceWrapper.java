package org.apollo.orm;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.log4j.Logger;
import org.apollo.orm.ApolloConstants.Util;


public class CassandraKeyspaceWrapper {
	public static final String CONF_CLUSTER_HOST = "cluster.host";
	public static final String CONF_CLUSTER_NAME = "cluster.name";
	public static final String CONF_CLUSTER_MAXACTIVE = "cluster.maxActive";
	public static final String CONF_CLUSTER_MAXIDLE = "cluster.maxIdle";
	public static final String CONF_CLUSTER_AUTO_DISCOVER_HOSTS = "cluster.autoDiscoverHosts";
	public static final String CONF_CLUSTER_THRIFT_SOCKET_TIMEOUT = "cluster.cassandraThriftSocketTimeout";
	public static final String CONF_CLUSTER_WAIT_TIMEOUT_WHEN_EXHAUSTED = "cluster.maxWaitTimeWhenExhausted";
	public static final String CONF_KEYSPACE = "keyspace";
	public static final String CONF_CACHE_CONF = "cache.conf";
	
	public static final String KEY_VALIDATION_CLASS = "validation_class";
	public static final String KEY_COLUMN_NAME = "column_name";
	public static final String KEY_DEFAULT_VALIDATION_CLASS = "default_validation_class";
	public static final String KEY_KEY_TYPE = "key_type";
	public static final String KEY_COLUMN_TYPE = "column_type";
	
	public static final String VAL_KEYS = "KEYS";
	public static final String VAL_SUPER = "Super";

	private static Logger logger = Logger.getLogger(CassandraKeyspaceWrapper.class);
	
	private Keyspace keyspace;

	private Cluster cluster;

	private Properties config;
	
	static Serializer<String> stringSerializer = StringSerializer.get();
	
	private String keyspace_name;
	
	private static CacheManager cacheManager;
	
	public CassandraKeyspaceWrapper(Properties config) throws Exception {
		this.config = config;
		
		init();
	}
	
	private void init() throws Exception {
		String host = config.getProperty(CONF_CLUSTER_HOST);
		
		String clustername = config.getProperty(CONF_CLUSTER_NAME);
		
		String keyspace = config.getProperty(CONF_KEYSPACE);
		
		keyspace_name = keyspace;
		
		int clusterMaxActive = getIntValue(config, CONF_CLUSTER_MAXACTIVE, 30) * 10;
		// int clusterMaxIdle = getIntValue(config, CONF_CLUSTER_MAXIDLE, 5);
		int cassandraThriftSocketTimeout = getIntValue(config, CONF_CLUSTER_THRIFT_SOCKET_TIMEOUT, 3000);
		int maxWaitTimeWhenExhausted = getIntValue(config, CONF_CLUSTER_WAIT_TIMEOUT_WHEN_EXHAUSTED, 4000);
		String cache_conf = config.getProperty(CONF_CACHE_CONF);
		boolean auto_discover_hosts = Util.getBooleanValue(config.getProperty(CONF_CLUSTER_AUTO_DISCOVER_HOSTS), false);
		
		logger.debug("host: " + host);
		logger.debug("clustername: " + clustername);
		logger.debug("clusterMaxActive: " + clusterMaxActive * 10);
		// logger.debug("clusterMaxIdle: " + clusterMaxIdle);
		logger.debug("cassandraThriftSocketTimeout: " + cassandraThriftSocketTimeout);
		logger.debug("maxWaitTimeWhenExhausted: " + maxWaitTimeWhenExhausted);
		logger.debug("cache_conf: " + cache_conf);
		logger.debug("keyspace_name: " + keyspace_name);
		
		CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
		
		cassandraHostConfigurator.setHosts(host);
		cassandraHostConfigurator.setMaxActive(clusterMaxActive);
		//cassandraHostConfigurator.setMaxIdle(clusterMaxIdle);
		//cassandraHostConfigurator.setRetryDownedHosts(true);
		cassandraHostConfigurator.setCassandraThriftSocketTimeout(cassandraThriftSocketTimeout);
		cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(maxWaitTimeWhenExhausted);	
		
		cassandraHostConfigurator.setAutoDiscoverHosts(auto_discover_hosts);
		
		cassandraHostConfigurator.setRetryDownedHosts(false);
		
		cluster = HFactory.createCluster(clustername, cassandraHostConfigurator);
		
		if (logger.isDebugEnabled())
			logger.debug("Cluster: " + cluster);
		
		//cluster = new ThriftCluster(clustername, cassandraHostConfigurator);
		
		//cluster.getKnownPoolHosts(true);
		
		this.keyspace = HFactory.createKeyspace(keyspace, cluster);
		
		logger.debug("Describing cluster ...");
		logger.debug(cluster.describeKeyspace(keyspace));
		
		if (cacheManager == null && cache_conf != null) {
			cacheManager = new CacheManager(cache_conf);
		}
	}
	
	public boolean doesColumnFamilyExist(String columnFamily) {
		KeyspaceDefinition ksd = null;
		
		Cache cache = null;
		
		if (cacheManager != null)
			cache = cacheManager.getCache("keyspaces_desc");
		
		if (cache != null) {
			Element e = cache.get(keyspace_name);
			
			if (e != null)
				ksd = (KeyspaceDefinition) e.getValue();
		}
		
		if (ksd == null) {
			ksd = cluster.describeKeyspace(keyspace_name);
			
			if (cache != null) {
				Element e = new Element(keyspace_name, ksd);
				
				cache.put(e);
			}
		}
		
		List<ColumnFamilyDefinition> cfs = ksd.getCfDefs();
		
		for (ColumnFamilyDefinition cf : cfs) {
			if (cf.getName().equals(columnFamily)) {
				return true;
			}
		}
		
		return false;
	}
	
	public static int getIntValue(Properties prop, String key, int _default) {
		int ret = _default;
		
		String _val = prop.getProperty(key);
		
		try {
			ret = Integer.parseInt(_val);
		} catch (NumberFormatException e) {
			logger.warn("Unable to make up an Integer from '" + _val + "'");
		}
		
		return ret;
	}
	
	public Cluster getCluster() {
		return cluster;
	}
	
	public String getKeyspaceName() {
		return keyspace_name;
	}
	
	public CassandraColumnFamilyWrapper createColumnFamily(String columnFamily) {
	    CassandraColumnFamilyWrapper ret = new CassandraColumnFamilyWrapper(this, columnFamily);
	    
	    ret.createColumnFamily();
	    
		return ret;
	}
	
	public CassandraColumnFamilyWrapper createSuperColumnFamily(String columnFamily) {
	    CassandraColumnFamilyWrapper ret = new CassandraColumnFamilyWrapper(this, columnFamily, true);
	    
	    ret.createColumnFamily();
	    
		return ret;
	}
	
	public String dropColumnFamily(String columnFamily) { 
	    String cfid2 = cluster.dropColumnFamily(keyspace.getKeyspaceName(), columnFamily);
	    
	    return cfid2;
	}
	
	public void truncateColumnFamily(String columnFamily) {
		cluster.truncate(keyspace.getKeyspaceName(), columnFamily);
	}
	
	public void addKeyspace(String keyspace) {
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace, "TmpCf");
		
	    cluster.addKeyspace(new ThriftKsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cfDef)));
	    
	    cluster.dropColumnFamily(keyspace, "TmpCf");
	}
	
	public void describeCluster() {
		logger.debug(cluster.describeClusterName());
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		
		for (KeyspaceDefinition kd : keyspaces) {
			String ks = kd.getName();
			
			logger.debug("Keyspace: " + ks);

			List<ColumnFamilyDefinition> cfs = kd.getCfDefs();
			
			for (ColumnFamilyDefinition cf : cfs) {
				logger.debug("CF: " + cf.getName());
				
				List<ColumnDefinition> cols = cf.getColumnMetadata();
				
				for (ColumnDefinition col : cols) {
					String colName = col.getName().toString();
					
					String colType = col.getIndexType().getDeclaringClass().getName();
					
					logger.debug("colName: " + colName + " colType: " + colType);
				}
			}
		}
	}
	
	public CassandraColumnFamilyWrapper getCassandraColumnFamilyWrapper(String columnFamily) {
		return getCassandraColumnFamilyWrapper(columnFamily, false);
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}

	public CassandraColumnFamilyWrapper getCassandraColumnFamilyWrapper(String columnFamily, boolean isSuper) {
		return new CassandraColumnFamilyWrapper(this, columnFamily, isSuper);
	}

	public void dropKeyspace(String someKeyspace) {
		cluster.dropKeyspace(someKeyspace);
	}
	
	public boolean doesKeyspaceExist(String keyspace) {
		return doesKeyspaceExist(cluster, keyspace);
	}
	
	public static boolean doesKeyspaceExist(Cluster cluster, String keyspace) {
		List<KeyspaceDefinition> keyspaces = getKeyspacesDefinition(cluster, keyspace);
		
		for (KeyspaceDefinition kd : keyspaces) {
			if (kd.getName().equals(keyspace))
				return true;
		}
		
		return false;
	}

	static List<KeyspaceDefinition> getKeyspacesDefinition(Cluster cluster, String keyspace) {
		List<KeyspaceDefinition> keyspaces = null;
		
		Cache cache = null;
		
		if (cacheManager != null) {
			cache = cacheManager.getCache("keyspaces");
		}
		
		if (cache != null) {
			Element e = cache.get(keyspace);
			
			keyspaces = (List<KeyspaceDefinition>) e.getValue();
		}
		
		if (keyspaces == null) {
			keyspaces = cluster.describeKeyspaces();
			
			if (cache != null) {
				Element e = new Element("keyspaces", keyspaces);

				cache.put(e);
			}
		}
		return keyspaces;
	}
}
