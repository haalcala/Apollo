package org.apollo.orm;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApolloIteratorImplTest {
	
	static Logger logger = LoggerFactory.getLogger(ApolloIteratorImplTest.class);
	
	static Properties conf;
	
	static CassandraKeyspaceWrapper keyspaceWrapper;
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		conf = TestConstants.Util.getTestConf();
		
		keyspaceWrapper = new CassandraKeyspaceWrapper(conf);
	}
	
	@Test
	public void testColumnIterator() throws Exception {
		int col_count = 220;
		
		List<String> data = new ArrayList<String>();
		
		String rowKey = TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString();
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper("TestTable");
		
		if (!keyspaceWrapper.doesColumnFamilyExist(cf.getColumnFamilyName()))
			cf.createColumnFamily();
		else
			keyspaceWrapper.truncateColumnFamily(cf.getColumnFamilyName());
		
		for (int i = 0; i < col_count; i++) {
			String dat = "" + i;
			
			data.add(dat);
			
			cf.insertColumn(rowKey, dat, "");
		}
		
		Iterator<String> it = ApolloConstants.Util.getApolloColumnIterator(cf, rowKey);
		
		assertTrue(it.hasNext());
		
		int c = 0;
		
		List<String> data2 = new ArrayList<String>(data);
		
		for (; it.hasNext();) {
			String dat = it.next();
			
			logger.debug("dat: " + dat + " c: " + (c++));
			
			assertNotNull(dat);
			
			assertTrue(data2.contains(dat));
			
			data2.remove(dat);
		}
		
		assertEquals(0, data2.size());
		
		assertEquals(col_count, c);
		
		cf.truncate();
	}
	
	@Test
	public void testRowIterator() throws Exception {
		List<String> data = new ArrayList<String>();
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper("TestTable");
		
		if (!keyspaceWrapper.doesColumnFamilyExist(cf.getColumnFamilyName()))
			cf.createColumnFamily();
		else
			keyspaceWrapper.truncateColumnFamily(cf.getColumnFamilyName());
		
		int row_count = 220;
		
		for (int i = 0; i < row_count; i++) {
			String dat = "row #" + i;
			
			data.add(dat);
			
			cf.insertColumn(dat, "col", ""+i);
		}
		
		Iterator<String> it = ApolloConstants.Util.getApolloRowIterator(cf);
		
		List<String> data2 = new ArrayList<String>(data);
		
		int c = 0;
		
		for (; it.hasNext(); ) {
			String dat = it.next();
			
			logger.debug("dat: " + dat + " c: " + (c++));
			
			assertNotNull(dat);
			
			assertTrue(data2.contains(dat));
			
			data2.remove(dat);
		}
		
		assertEquals(0, data2.size());
		assertEquals(row_count, c);
		
		cf.truncate();
	}
}
