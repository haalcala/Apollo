package org.apollo.orm;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CriteriaImplTest {
	Logger logger = LoggerFactory.getLogger(CriteriaImplTest.class);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetOrderedKeys() {
		List<Order> order = new ArrayList<Order>();
		
		order.add(Order.asc("col 1"));
		
		Map<String, Map<String, Serializable>> rows = new LinkedHashMap<String, Map<String,Serializable>>();
		
		Map<String, Serializable> cols;
		int rowKey = 1, colKey = 1;

		
		cols = new LinkedHashMap<String, Serializable>();
		rowKey = 1;
		rows.put("row key " + rowKey, cols);
		
		colKey = 1;
		cols.put("col " + colKey, "z");
		
		
		cols = new LinkedHashMap<String, Serializable>();
		rowKey = 1000;
		rows.put("row key " + rowKey, cols);
		
		colKey = 1;
		cols.put("col " + colKey, "A");
		
		
		cols = new LinkedHashMap<String, Serializable>();
		rowKey = 3;
		rows.put("row key " + rowKey, cols);
		
		colKey = 1;
		cols.put("col " + colKey, "c");
		
		
		cols = new LinkedHashMap<String, Serializable>();
		rowKey = 101;
		rows.put("row key " + rowKey, cols);
		
		colKey = 1;
		cols.put("col " + colKey, "1");
		
		
		cols = new LinkedHashMap<String, Serializable>();
		rowKey = 100;
		rows.put("row key " + rowKey, cols);
		
		colKey = 1;
		cols.put("col " + colKey, "a");
		
		List<String> orderedKeys = CriteriaImpl.getOrderedKeys(order, rows);
		
		logger.info("" + rows);
		logger.info("" + orderedKeys);
		
		Iterator<String> it = orderedKeys.iterator();
		
		/*
		 * Nubers first, then capital letters and then small letters
		 */
		assertEquals("row key 101", it.next());
		assertEquals("row key 1000", it.next());
		assertEquals("row key 100", it.next());
		assertEquals("row key 3", it.next());
		assertEquals("row key 1", it.next());
	}

}
