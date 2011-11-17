package org.apollo.orm;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectMapperTest {
	
	static Logger logger = LoggerFactory.getLogger(ObjectMapperTest.class);
	
	private static final String STR_KEYSPACE = "TestApollo";
	
	private static final int replicationFactor = 1;
	private static final String columnFamily = "MyTestCF";

	private static final String COL_DOMAIN = "domain";
	private static final String COL_TIME = "time";
	private static final String COL_DATE = "date";
	
	@Test
	public void test() {
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster","localhost:9160");
		
		/*
		try {
			cluster.dropColumnFamily(STR_KEYSPACE, columnFamily);
		} catch (HectorException e1) {
		}
		*/
		
		//Add the schema to the cluster.
		//"true" as the second param means that Hector will block until all nodes see the change.
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(STR_KEYSPACE);
		
		logger.debug("keyspaceDef: " + keyspaceDef);

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(STR_KEYSPACE,
				columnFamily,
				ComparatorType.ASCIITYPE);
		
		BasicColumnDefinition bcd = new BasicColumnDefinition();
		bcd.setName(ByteBufferUtil.bytes(COL_TIME));
		bcd.setValidationClass(LongType.class.getCanonicalName());
		
		// If keyspace does not exist, the CFs don't exist either. => create them.
		if (keyspaceDef == null) {
			keyspaceDef = HFactory.createKeyspaceDefinition(STR_KEYSPACE,
					ThriftKsDef.DEF_STRATEGY_CLASS,
					replicationFactor,
					Arrays.asList(cfDef));

			cluster.addKeyspace(keyspaceDef, true);
			
			cfDef.addColumnDefinition(bcd);
			cluster.updateColumnFamily(cfDef);
			
		    createSchema();
		}
		
		List<ColumnFamilyDefinition> cfDefs = keyspaceDef.getCfDefs();
		
		boolean found_cf = false;
		
		for (ColumnFamilyDefinition _cfDef : cfDefs) {
			if (_cfDef.getName().equals(columnFamily)) {
				found_cf = true;
				cfDef = _cfDef;
				break;
			}
		}
		
		if (!found_cf) {
			cluster.addColumnFamily(cfDef);
		}
		
		Keyspace ksp = HFactory.createKeyspace(STR_KEYSPACE, cluster);
		
		ColumnFamilyTemplate<String, String> template =
                new ThriftColumnFamilyTemplate<String, String>(ksp,
                                                               columnFamily,
                                                               StringSerializer.get(),
                                                               StringSerializer.get());
		
		// <String, String> correspond to key and Column name.
		String key = "a key";
		
		String val_domain = "www.datastax.com";
		long val_time = 1234567890l;
		
		Date val_date = new Date(val_time);
		
		{
			ColumnFamilyResult<String, String> res = template.queryColumns(key);
			
			logger.debug("res: " + res + " " + res.getLong(COL_TIME) + " " + res.getLong(COL_DOMAIN));
			
			ColumnFamilyUpdater<String, String> updater = template.createUpdater(key);
			updater.setString(COL_DOMAIN, val_domain);
			updater.setLong(COL_TIME, val_time);
			updater.setDate(COL_DATE, val_date);

			try {
				template.update(updater);
			} catch (HectorException e) {
				// do something ...
			}
		}
		
		try {
		    ColumnFamilyResult<String, String> res = template.queryColumns(key);
		    String value = res.getString(COL_DOMAIN);
		    long value2 = res.getLong(COL_TIME);
		   
		    assertEquals(val_domain, value);
		    
		    assertEquals(val_time, value2);
		} catch (HectorException e) {
		    // do something ...
		}
		
		try {
		    template.deleteColumn(key, COL_DOMAIN);
		    
		    ColumnFamilyResult<String, String> res = template.queryColumns(key);
		    String value = res.getString(COL_DOMAIN);
		   
		    assertNull(value);
		} catch (HectorException e) {
		    // do something
		}
	}

	private void createSchema() {
		// TODO Auto-generated method stub
		
	}
}
