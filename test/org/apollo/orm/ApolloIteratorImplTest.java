package org.apollo.orm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

public class ApolloIteratorImplTest {
	
	static Logger logger = Logger.getLogger(ApolloIteratorImplTest.class);
	
	@Test
	public void test() throws Exception {
		Properties conf = TestConstants.Util.getTestConf();
		
		CassandraKeyspaceWrapper keyspaceWrapper = new CassandraKeyspaceWrapper(conf);
		
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
		
		Iterator<String> it = new ApolloIteratorImpl(cf, null, rowKey, "", "");
		
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
		
		
	}
}
