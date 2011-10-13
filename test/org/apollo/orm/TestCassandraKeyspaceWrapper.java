package org.apollo.orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import me.prettyprint.hector.api.Keyspace;

import org.apache.log4j.Logger;
import org.apollo.orm.CassandraColumnFamilyWrapper;
import org.apollo.orm.CassandraKeyspaceWrapper;
import org.apollo.orm.GetColumnsHandlerAdapter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author   harold
 */
public class TestCassandraKeyspaceWrapper implements TestConstants {
	static Logger logger = Logger.getLogger(TestCassandraKeyspaceWrapper.class);
	
	private static Properties config;
	
	/**
	 * @uml.property  name="keyspace"
	 * @uml.associationEnd  
	 */
	private static CassandraKeyspaceWrapper keyspace;
	
	@BeforeClass
	public static void prep() throws Exception {
		config = Util.getTestConf();
		
		keyspace = new CassandraKeyspaceWrapper(config);
	}
	
	@Before
	public void init() throws IOException {
	}
	
	@AfterClass
	public static void tearDown() {
	}

	@Test
	public void testGetIntValue() {
		Properties prop = new Properties();
		
		String key = "just_another_key";
		int val = 3414141;
		
		prop.setProperty(key, "" + val);
		
		int _default = 123456;
		int ret = CassandraKeyspaceWrapper.getIntValue(prop, key, _default);
		assertEquals(val, ret);
		
		prop = new Properties();
		prop.setProperty(key + "aa", "" + val);
		ret = CassandraKeyspaceWrapper.getIntValue(prop, key, _default);
		assertEquals(_default, ret);
		
		prop = new Properties();
		ret = CassandraKeyspaceWrapper.getIntValue(prop, key, _default);
		assertEquals(_default, ret);
	}

	@Test
	public void testGetCluster() {
		assertNotNull(keyspace.getCluster());
	}

	@Test
	public void testGetKeyspaceName() {
		assertEquals(config.getProperty(CassandraKeyspaceWrapper.CONF_KEYSPACE), keyspace.getKeyspaceName());
	}

	@Test
	public void testCreateColumnFamily() throws Exception {
		String someCF = "someStandardCF";
		
		
		CassandraColumnFamilyWrapper cf = null;
		
		try {
			cf = keyspace.createColumnFamily(someCF);
		} 
		catch (Exception e) {
			if (e.getMessage().indexOf("CF is already defined") < 0)
				throw e;
			else
				cf = keyspace.getCassandraColumnFamilyWrapper(someCF, false);
		}
		
		assertNull(cf.getColumnValue("someKey", "someColumn"));
		
		keyspace.dropColumnFamily(someCF);
	}

	@Test
	public void testCreateSuperColumnFamily() throws Exception {
		String someCF = "someSuperCF";
		
		CassandraColumnFamilyWrapper cf = null;
		
		try {
			cf = keyspace.createSuperColumnFamily(someCF);
		} 
		catch (Exception e) {
			if (e.getMessage().indexOf("CF is already defined") < 0)
				throw e;
			else
				cf = keyspace.getCassandraColumnFamilyWrapper(someCF, true);
		}
		
		String someStartKey = "someStartKey";
		String someEndKey = "";
		String[] someColumns = new String[] {"someColumn"};
		
		assertNull(cf.getSuperColumns(someStartKey, someEndKey, someColumns));
		
		keyspace.dropColumnFamily(someCF);
	}

	@Test
	public void testDropColumnFamily() throws Exception {
		String someCF = "someCF";
		
		if (!keyspace.doesColumnFamilyExist(someCF))
			keyspace.createColumnFamily(someCF);
		
		try {
			keyspace.dropColumnFamily(someCF);
		} 
		catch (Exception e) {
			if (e.getMessage().indexOf("CF is not defined") > -1)
				throw e;
		}
	}

	@Test
	public void testTruncateColumnFamily() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspace.createColumnFamily(someCF);
		
		String someValue = "someValue";
		String someColumn = "someColumn";
		String someKey = "someKey";
		
		cf.insertColumn(someKey, someColumn, someValue);
		
		assertEquals(someValue, cf.getColumnValue(someKey, someColumn));
		
		keyspace.truncateColumnFamily(someCF);
		
		assertNull(cf.getColumnValue(someKey, someColumn));
	}
	
	@Test
	public void testAddKeyspace() {
		String someKeyspace = "someKeyspace";

		if (!keyspace.doesKeyspaceExist(someKeyspace))
			keyspace.addKeyspace(someKeyspace);
		
		assertTrue(keyspace.doesKeyspaceExist(someKeyspace));
		
		keyspace.dropKeyspace(someKeyspace);
	}
	
	@Test
	public void testDropKeyspace() {
		String someKeyspace = "someKeyspace";
		
		assertTrue(!keyspace.doesKeyspaceExist(someKeyspace));
	}

	@Test
	public void testGetCassandraColumnFamilyWrapperString() {
		String someColumnFamily = "somCF";
		
		CassandraColumnFamilyWrapper cf = keyspace.getCassandraColumnFamilyWrapper(someColumnFamily);
		
		assertNotNull(cf);
	}

	@Test
	public void testGetKeyspace() {
		Keyspace ks = keyspace.getKeyspace();
		
		assertNotNull(ks);
	}
	
	@Test
	public void testDataOrdering() {
		String cfWithAsciiComparator = "cfWithAsciiComparator";
		
		CassandraColumnFamilyWrapper cf = keyspace.getCassandraColumnFamilyWrapper(cfWithAsciiComparator);
		
		if (keyspace.doesColumnFamilyExist(cfWithAsciiComparator))
			keyspace.dropColumnFamily(cfWithAsciiComparator);

		//cf.setIntegerComparator();
		cf.createColumnFamily();
		
		int maxRows = 10;
		
		int maxCols = 10;
		
		for (int i = 0; i < maxRows; i++) {
			for (int j = maxCols; j > 0; j--) {
				
				String key = "" + (i < 10 ? "0" + i : i);
				
				String col = "" + (j < 10 ? "0" + j : j);
				
				cf.insertColumn(key, col, col);
				
			}
		}
		
		cf.getColumns("", "", "", "", maxCols, maxRows, new GetColumnsHandlerAdapter() {

			@Override
			public boolean scanAllRows() {
				return super.scanAllRows();
			}

			@Override
			public boolean scanAllColumns() {
				return super.scanAllColumns();
			}

			@Override
			public void onStartRow(String rowKey) {
				logger.debug("Row Key : " + rowKey);
			}

			@Override
			public void onEndRow(String rowKey) {
				super.onEndRow(rowKey);
			}

			@Override
			public void onColumn(String key, String val) {
				super.onColumn(key, val);
			}

			@Override
			public int getColumnsPerRequest() {
				return super.getColumnsPerRequest();
			}

			@Override
			public int getRowsPerRequest() {
				return super.getRowsPerRequest();
			}

			@Override
			public boolean skipRow() {
				return super.skipRow();
			}

			@Override
			public boolean scanRows() {
				return super.scanRows();
			}

			@Override
			public boolean scanCols() {
				return super.scanCols();
			}

			@Override
			public boolean skipCol() {
				return super.skipCol();
			}
		});
	}

	@Test
	public void doNothing() {
	}
}
