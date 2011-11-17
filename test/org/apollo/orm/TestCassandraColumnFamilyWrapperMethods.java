package org.apollo.orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.swing.RowFilter.ComparisonType;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.log4j.Logger;
import org.apollo.orm.CassandraColumnFamilyWrapper;
import org.apollo.orm.CassandraKeyspaceWrapper;
import org.apollo.orm.GetColumnsHandlerAdapter;
import org.apollo.orm.GetSuperColumnsHandlerAdapter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author   harold
 */
public class TestCassandraColumnFamilyWrapperMethods {
	/**
	 */
	private Logger logger = Logger.getLogger(getClass());

	private static Properties config;
	/**
	 * @uml.property  name="keyspaceWrapper"
	 * @uml.associationEnd  
	 */
	private static CassandraKeyspaceWrapper keyspaceWrapper;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		config = TestConstants.Util.getTestConf();
		
		keyspaceWrapper = new CassandraKeyspaceWrapper(config);
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
	public void testCassandraColumnFamilyWrapperCassandraKeyspaceWrapperStringBoolean() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF, false);
		
		assertFalse(cf.isSuper());
		
		cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF, true);
		
		assertTrue(cf.isSuper());
		
		assertEquals(someCF, cf.getColumnFamilyName());
	}

	@Test
	public void testCassandraColumnFamilyWrapperCassandraKeyspaceWrapperString() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		assertFalse(cf.isSuper());
		
		assertEquals(someCF, cf.getColumnFamilyName());
	}

	@Test
	public void testInsertGetColumn() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (!keyspaceWrapper.doesColumnFamilyExist(someCF))
			cf.createColumnFamily();
		
		String someValue = "someValue";
		String someColumn = "someColumn";
		String someKey = "someKey";
		
		cf.insertColumn(someKey, someColumn, someValue);
		
		assertEquals(someValue, cf.getColumnValue(someKey, someColumn));
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	@Test
	public void testCreateColumnFamily() {
		String someCF = "someCF";
		
		assertFalse(keyspaceWrapper.doesColumnFamilyExist(someCF));
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		cf.createColumnFamily();
		
		assertTrue(keyspaceWrapper.doesColumnFamilyExist(someCF));
	}

	@Test
	public void testSetASCIIComparator() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		cf.setASCIIComparator();
		
		assertEquals(ComparatorType.ASCIITYPE, cf.getComparator());
	}

	@Test
	public void testSetUTF8Comparator() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		cf.setIntegerComparator();
		
		assertEquals(ComparatorType.INTEGERTYPE, cf.getComparator());
	}

	@Test
	public void testSetLongComparator() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		cf.setLongComparator();
		
		assertEquals(ComparatorType.LONGTYPE, cf.getComparator());
	}

	@Test
	public void testSetIntegerComparator() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		cf.setIntegerComparator();
		
		assertEquals(ComparatorType.INTEGERTYPE, cf.getComparator());
	}

	@Test
	public void testSetUUIDComparator() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		cf.setUUIDComparator();
		
		assertEquals(ComparatorType.UUIDTYPE, cf.getComparator());
	}

	@Test
	public void testSetTIMEUUIDComparator() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		cf.setTIMEUUIDComparator();
		
		assertEquals(ComparatorType.TIMEUUIDTYPE, cf.getComparator());
	}
	
	@Test
	public void testUpdateColumnFamilyMetaData() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (!keyspaceWrapper.doesColumnFamilyExist(someCF))
			cf.createColumnFamily();
		else
			cf.truncate();
		
		cf.updateColumnFamilyMetaData("col1", null, LongType.instance);
		
		assertEquals(LongSerializer.get(), cf.getColumnSerializer("col1", true));
		
		Object[][] data = {{"key1", "col1", 1l}, {"key2", "col1", 5l}
							, {"key3", "col1", 55l}, {"key4", "col1", 8l}
							, {"key5", "col1", Long.MIN_VALUE}, {"key6", "col1", Long.MAX_VALUE}
							};
		
		for (Object[] objects : data) {
			String rowKey = (String) objects[0];
			String colKey = (String) objects[1];
			long val = (Long) objects[2];
			
			cf.insertColumn(rowKey, colKey, val);
			cf.insertColumn(rowKey, "__rstat__", 0);
		}
		
		for (Object[] objects : data) {
			String rowKey = (String) objects[0];
			String colKey = (String) objects[1];
			Long exp_val = (Long) objects[2];
			
			Long val = (Long) cf.getColumnValue(rowKey, colKey);
			
			assertEquals(exp_val, val);
		}
	}

	@Test
	public void testInsertGetListSuperColumn() {
		String someCF = "someSuperCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF, true);
		
		if (keyspaceWrapper.doesColumnFamilyExist(someCF))
			keyspaceWrapper.dropColumnFamily(someCF);
			
		cf.createColumnFamily();
		
		Map<String, String> kvp = new LinkedHashMap<String, String>();
		
		String someRowKey = "someRowKey";
		String[] keys = {"a1", "b2", "c3"};
		String[] vals = {"aa11", "bb22", "cc33"};
		
		for (int i = 0; i < keys.length; i++) {
			String k = keys[i];
			String v = vals[i];
			
			kvp.put(k, v);
		}
		
		String someColumn = "someColumn";
		
		cf.insertSuperColumn(someRowKey, someColumn, kvp);
		
		for (int i = 0; i < keys.length; i++) {
			String k = keys[i];
			String v = vals[i];

			String ret = cf.getSuperColumnValue(someRowKey, someColumn, k);
			
			assertEquals(v, ret);
		}
		
		Map<String, Map<String, Map<String, String>>> rows = cf.getSuperColumns("", "", new String[] {someColumn});
		
		assertNotNull(rows);
		
		for (String rowKey : rows.keySet()) {
			Map<String, Map<String, String>> scols = rows.get(rowKey);
			
			for (String colKey : scols.keySet()) {
				Map<String, String> cols = scols.get(colKey);
				
				for (String col : cols.keySet()) {
					if (kvp.containsKey(col)) {
						String val = cols.get(col);
						
						assertEquals(kvp.get(col), val);
						
						kvp.remove(col);
					}
				}
			}
		}
		
		assertEquals(0, kvp.size());
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	public void testInsertGetListSuperColumnHandler() {
		String someCF = "someSuperCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF, true);
		
		if (keyspaceWrapper.doesColumnFamilyExist(someCF))
			keyspaceWrapper.dropColumnFamily(someCF);
		
		cf.createColumnFamily();
		
		int maxRows = 25;
		int maxSuperCols = 25;
		int maxCols = 25;
		String[] columns = new String[maxSuperCols];
		
		final Map<String, Map<String, Map<String, String>>> data = new LinkedHashMap<String, Map<String,Map<String,String>>>();
		
		for (int i = 0; i < maxRows; i++) {
			String rowKey = "rowKey_" + (i < 10 ? "0" + i : i);
			
			Map<String, Map<String, String>> sCol = new LinkedHashMap<String, Map<String,String>>();

			data.put(rowKey, sCol);

			for (int j = 0; j < maxSuperCols; j++) {
				String superColKey = "superColKey_" + (j < 10 ? "0" + j : j);
				
				columns[j] = superColKey;
				
				Map<String, String> cols = new LinkedHashMap<String, String>();
				
				sCol.put(superColKey, cols);
				
				for (int j2 = 0; j2 < maxCols; j2++) {
					String col = "col_" + (j2 < 10 ? "0" + j2 : j2);
					String val = "val_" + (j2 < 10 ? "0" + j2 : j2);
					
					cols.put(col, val);
				}
				
				cf.insertSuperColumn(rowKey, superColKey, cols);
			}
		}
		
		for (String rowKey : data.keySet()) {
			Map<String, Map<String, String>> superCols = data.get(rowKey);
			
			for (String superColKey : superCols.keySet()) {
				Map<String, String> cols = superCols.get(superColKey);
				
				for (String col : cols.keySet()) {
					String val = cols.get(col);
					
					String exp = cf.getSuperColumnValue(rowKey, superColKey, col);
					
					assertEquals(val, exp);
				}
			}
		}
		
		class Counter {
			int totalRows;
			
			int totalSuperCols;
			
			int totalCols;
			
			int totalEndSuperCols;
			
			int totalEndRows;
			
			int tmpCountedRows;
			
			int tmpTotalSuperCols;
			
			int tmpTotalCols;
			
			String startKey = "";
		}
		
		final Counter counter = new Counter();
		final int rpp = 10;
		final int maxRows2 = 15;
		
		do {
			counter.tmpCountedRows = 0;
			
			cf.getSuperColumns(counter.startKey, "", columns, maxRows2 + 1, new GetSuperColumnsHandlerAdapter() {
				int bypassRows;
				
				private Map<String, Map<String, String>> scols;

				private Map<String, String> cols;
				
				@Override
				public void onStartRow(String rowKey) {
					if (rowKey.equals(counter.startKey)) {
						log("Found " + rowKey + " again");
						
						bypassRows = 1;
						return;
					}
					
					scols = data.get(rowKey);
					
					assertNotNull(scols);
					
					counter.totalRows++;
					counter.tmpCountedRows++;
					counter.startKey = rowKey;
					
					log("onStartRow: RowKey: " + rowKey + " totalRows: " + counter.totalRows + " tmpCountedRows: " + counter.tmpCountedRows);
				}

				@Override
				public void onStartSuperColumn(String superColumnName) {
					//log("onStartSuperColumn: superColumnName: " + superColumnName);
					
					cols = scols.get(superColumnName);
					
					assertNotNull(cols);

					counter.totalSuperCols++;
				}

				@Override
				public void onColumn(String columnName, String columnValue) {
					// log("Column Name: '" + columnName + "' Value: '" + columnValue + "'");
					
					String _val = cols.get(columnName);
					
					assertNotNull(_val);
					assertEquals(_val, columnValue);
					
					cols.remove(columnName);

					counter.totalCols++;
				}

				@Override
				public void onEndSuperColumn(String superColumnName) {
					//log("onEndSuperColumn: superColumnName: " + superColumnName);
					
					assertEquals(0, cols.size());
					
					scols.remove(superColumnName);

					counter.totalEndSuperCols++;
				}

				@Override
				public void onEndRow(String rowKey) {
					//log("onEndRow: rowKey: " + rowKey);
					
					assertEquals(0, scols.size());
					
					data.remove(rowKey);

					counter.totalEndRows++;
				}

				@Override
				public int getRowsPerPage() {
					return rpp  + 1;
				}

				@Override
				public boolean skipRow() {
					if (bypassRows > 0) {
						bypassRows--;
						return true;
					}
					
					return super.skipRow();
				}

				void log(String msg) {
					logger.debug("Custome Handler: " + msg);
				}
			});
				
		} while (counter.tmpCountedRows >= rpp);
		
		assertEquals(0, data.size());
		assertEquals(maxRows, counter.totalRows);
		assertEquals(maxRows, counter.totalEndRows);
		
		assertEquals(maxRows * maxSuperCols, counter.totalSuperCols);
		assertEquals(maxRows * maxSuperCols, counter.totalEndSuperCols);
		
		assertEquals(maxRows * maxSuperCols * maxCols, counter.totalCols);
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}
	
	@Test
	public void testGetMultipleColumns() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (!keyspaceWrapper.doesColumnFamilyExist(someCF))
			cf.createColumnFamily();
		
		String someRowKey = "someRowKey";
		String[] keys = {"a1", "b2", "c3"};
		String[] vals = {"aa11", "bb22", "cc33"};
		
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];
			String val = vals[i];
			
			cf.insertColumn(someRowKey, key, val);
		}
		
		Map<String, String> cols = cf.getMultipleColumns(someRowKey, keys);
		
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];
			String val = cols.get(key);
			
			assertEquals(vals[i], val);
		}
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	@Test
	public void testGetRowsWithMultipleKeysStringArrayStringStringInt() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (keyspaceWrapper.doesColumnFamilyExist(someCF)) {
			keyspaceWrapper.dropColumnFamily(someCF);
		}
		
		cf.setUTF8Comparator();
		cf.createColumnFamily();
		
		String[] someRowKeys = {"someRowKey1", "someRowKey2"};
		
		int colCount = 100;
		
		for (String someRowKey : someRowKeys) {
			for (int i = 0; i < colCount; i++) {
				String key = "key_" + i;
				String val = "val_" + i;
				
				cf.insertColumn(someRowKey, key, val);
			}
		}
		
		Map<String, Map<String, String>> cols = cf.getRowsWithMultipleKeys(someRowKeys, "key_0", "", colCount);
		
		assertEquals(2, cols.size());
		
		for (String someRowKey : someRowKeys) {
			Map<String, String> _cols = cols.get(someRowKey);
			
			assertNotNull(_cols);
			
			for (int i = 0; i < colCount; i++) {
				String key = "key_" + i;
				String val = _cols.get(key);
				
				assertEquals("val_" + i, val);
			}
		}
		
		Map<String, Map<String, String>> __cols = cols;
		
		int colCount2 = 35;
		
		for (String someRowKey : someRowKeys) {
			int colsFound = 0;
			String lastCol = "";
			
			Map<String, String> _col = __cols.get(someRowKey);
			
			do {
				logger.debug("lastCol: " + lastCol);
				
				colsFound = 0;
				cols = cf.getRowsWithMultipleKeys(new String[] {someRowKey}, lastCol, "", colCount2 + 1);

				assertEquals(1, cols.size());

				Map<String, String> _cols = cols.get(someRowKey);

				assertNotNull(_cols);
				
				logger.debug(someRowKey + "("+_cols.size()+")" + " " + _cols);

				for (String col : _cols.keySet()) {
					_col.remove(col);
					
					lastCol = col;
					
					colsFound++;
				}
				
				logger.debug("Columns found: " + colsFound);
			} while (colsFound >= colCount2);
			
			assertEquals(0, _col.size());
		}
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	@Test
	public void testGetRowsWithMultipleKeysStringArray() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (!keyspaceWrapper.doesColumnFamilyExist(someCF)) {
			cf.setUTF8Comparator();
			cf.createColumnFamily();
		}
		
		String[] someRowKeys = {"someRowKey1", "someRowKey2"};
		
		int colCount = 99;
		
		for (String someRowKey : someRowKeys) {
			for (int i = 0; i < colCount; i++) {
				String key = "key_" + i;
				String val = "val_" + i;
				
				cf.insertColumn(someRowKey, key, val);
			}
		}
		
		Map<String, Map<String, String>> cols = cf.getRowsWithMultipleKeys(someRowKeys);
		
		assertEquals(2, cols.size());
		
		for (String key : cols.keySet()) {
			logger.debug(key);
		}
		
		for (String someRowKey : someRowKeys) {
			Map<String, String> _cols = cols.get(someRowKey);
			
			logger.debug(someRowKey + " cols: " + _cols);
			
			assertNotNull(_cols);
			
			for (int i = 0; i < colCount; i++) {
				String key = "key_" + i;
				String val = _cols.get(key);
				
				assertEquals("val_" + i, val);
			}
		}
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	@Test
	public void testDeleteRow() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (!keyspaceWrapper.doesColumnFamilyExist(someCF)) {
			cf.setUTF8Comparator();
			cf.createColumnFamily();
		}
		
		String someValue = "someValue";
		String someColumn = "someColumn";
		String someRowKey = "someRowKey";
		
		cf.insertColumn(someRowKey, someColumn, someValue);
		
		assertEquals(someValue, cf.getColumnValue(someRowKey, someColumn));
		
		cf.deleteRow(someRowKey);
		
		assertNull(cf.getColumnValue(someRowKey, someColumn));
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	@Test
	public void testGetColumnsAsMap() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (keyspaceWrapper.doesColumnFamilyExist(someCF))
			keyspaceWrapper.dropColumnFamily(someCF);
		
		cf.setUTF8Comparator();
		cf.createColumnFamily();
		
		int maxRows = 25;
		int maxCols = 25;
		
		Map<String, Map<String, Serializable>> values = new LinkedHashMap<String, Map<String,Serializable>>();
		
		for (int i = 0; i < maxRows; i++) {
			String key = "key_" + i;
			
			Map<String, Serializable> _columns = new LinkedHashMap<String, Serializable>();
			
			for (int j = 0; j < maxCols; j++) {
				String col = "col_" + j;
				Serializable val = "val_" + j;
				
				_columns.put(col, val);
				
				cf.insertColumn(key, col, val);
			}
			
			values.put(key, _columns);
		}
		
		String endCol = "";
		String startCol = "";
		String endKey = "";
		String startKey = "";
		
		int maxCols2 = 10;
		int maxRows2 = 10;
		
		int rowsFound = 0;
		int colsFound = 0;
		
		int totalRowsFound = 0;
		int totalColsFound = 0;
		
		do {
			rowsFound = 0;
			startCol = "";
			
			Map<String, Map<String, Serializable>> rows = cf.getColumnsAsMap(startKey, endKey, startCol, endCol, maxCols2, maxRows2);
			
			for (String rowKey : rows.keySet()) {
				
				totalColsFound = 0;
				rowsFound++;
				startKey = rowKey;
				
				Map<String, Serializable> cols = rows.get(rowKey);
				
				Map<String, Serializable> _cols = values.get(rowKey);

				do {
					colsFound = 0;
					
					logger.debug(rowKey + "("+cols.size()+"/"+totalColsFound+" st: "+startCol+")");

					for (String col : cols.keySet()) {
						startCol = col;

						colsFound++;
						
						logger.debug("Found a column : " + col);
						
						if (colsFound >= maxCols2)
							break;

						_cols.remove(col);
						
						totalColsFound++;
					}
					
					if (colsFound >= maxCols2) {
						Map<String, Map<String, Serializable>> _rows = cf.getColumnsAsMap(startKey, endKey, startCol, endCol, maxCols2, maxRows2 + 1);
						cols = _rows.get(rowKey);
					}
				} while (colsFound >= maxCols2);
				
				if (_cols.size() > 0) {
					logger.debug("Remaining cols: ");
					for (String col : _cols.keySet()) {
						logger.debug("Col: " + col);
					}
				}
				
				assertEquals(0, _cols.size());
				assertEquals(maxCols, totalColsFound);
				
				logger.debug("rowsFound: " + rowsFound + " startKey: " + startKey);
				
				if (rowsFound >= maxRows2)
					break;
				
				values.remove(rowKey);
				
				totalRowsFound++;
			}
		} while (rowsFound >= maxRows2);
		
		assertEquals(maxRows, totalRowsFound);
		assertEquals(0, values.size());
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	@Test
	public void testGetColumnsWithHandler() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		if (keyspaceWrapper.doesColumnFamilyExist(someCF))
			keyspaceWrapper.dropColumnFamily(someCF);
		
		cf.setUTF8Comparator();
		cf.createColumnFamily();
		
		int maxRows = 25;
		int maxCols = 25;
		
		final Map<String, Map<String, String>> values = new LinkedHashMap<String, Map<String,String>>();
		
		for (int i = 0; i < maxRows; i++) {
			String key = "key_" + (i < 10 ? "0" + i : i);
			
			Map<String, String> _columns = new LinkedHashMap<String, String>();
			
			for (int j = 0; j < maxCols; j++) {
				String col = "col_" + (j < 10 ? "0" + j : j);
				String val = "val_" + (j < 10 ? "0" + j : j);
				
				_columns.put(col, val);
				
				cf.insertColumn(key, col, val);
			}
			
			values.put(key, _columns);
		}
		
		final int maxRows2 = 10;
		final int maxCols2 = 10;
		String endColumn = "";
		String startColumn = "";
		String endKey = "";
		String startKey = "";
		
		class Counter {
			int tmpRowCount;
			
			int tmpColCount;
			
			String startKey = "";
			
			String startCol = "";

			public int maxRows = maxRows2 + 1;
		}
		
		final Counter c = new Counter();
		
		final Map<String, String> rowKeyLastCol = new LinkedHashMap<String, String>();
		
		do {
			if (rowKeyLastCol.size() > 0) {
				c.startKey = rowKeyLastCol.keySet().iterator().next();
				c.startCol = rowKeyLastCol.get(c.startKey);
				c.maxRows = 1;
				
				rowKeyLastCol.remove(c.startKey);
				
				logger.debug("Searching for more columns for rowKey: " + c.startKey);
			}
			
			do {
				c.tmpRowCount = 0;

				cf.getColumns(c.startKey, endKey, c.startCol, endColumn, maxCols2 + 1, c.maxRows, null, new GetColumnsHandlerAdapter() {
					private int skipRows;
					private int skipCols;
					private Map<String, String> cols;

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
						c.startKey = rowKey;

						c.tmpRowCount++;

						c.tmpColCount = 0;

						if (c.tmpRowCount > maxRows2) {
							skipRows = 1;
							return;
						}
						
						cols = values.get(rowKey);
						
						assertNotNull(cols);

						log("START ROW: RowKey: " + rowKey + " tmpRowCount: " + c.tmpRowCount);
					}

					@Override
					public void onEndRow(String rowKey) {
						log("END ROW: RowKey: " + rowKey);
					}

					@Override
					public void onColumn(String col, Serializable val) {
						c.tmpColCount++;

						//c.startCol = col;

						if (c.tmpColCount > maxCols2) {
							skipCols = 1;

							rowKeyLastCol.put(c.startKey, col);

							log("Row Key: " + c.startKey + " col: " + col + " tmpColCount: " + c.tmpColCount + ". Skipping.");

							return;
						}
						
						String _val = cols.get(col);
						
						assertNotNull(_val);
						assertEquals(_val, val);
						
						cols.remove(col);

						log("Row Key: " + c.startKey + " col: " + col + " tmpColCount: " + c.tmpColCount);
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
						if (skipRows > 0) {
							skipRows--;
							return true;
						}

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
						if (skipCols > 0) {
							skipCols--;
							return true;
						}

						return super.skipCol();
					}

					void log(String msg) {
						logger.debug("Custom Handler: " + msg);
					}
				});
			} while (c.tmpRowCount >= maxRows2);
		} while (rowKeyLastCol.size() > 0);
		
		for (String key : values.keySet()) {
			Map<String, String> cols = values.get(key);
			
			assertEquals(0, cols.size());
		}
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}
	
	@Test
	public void testIsSuper() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);

		assertFalse(cf.isSuper());
		
		cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF, false);

		assertFalse(cf.isSuper());
		
		cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF, true);
		
		assertTrue(cf.isSuper());
	}

	@Test
	public void testGetColumnFamilyName() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);

		assertEquals(someCF, cf.getColumnFamilyName());
	}

	@Test
	public void testBuildColumnDefs() {
		String someCF = "someCF";
		
		if (keyspaceWrapper.doesColumnFamilyExist(someCF))
			keyspaceWrapper.dropColumnFamily(someCF);
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);

		Map<String, Map<String, Map<String, String>>> cfs = new LinkedHashMap<String, Map<String,Map<String,String>>>();
		
		Map<String, Map<String, String>> columnDefs = null;
		
		Map<String, String> col = null;
		
		columnDefs = new LinkedHashMap<String, Map<String,String>>();
		cfs.put(someCF, columnDefs);
		col = new LinkedHashMap<String, String>();
		columnDefs.put("col1", col);
		
		col.put(CassandraKeyspaceWrapper.KEY_VALIDATION_CLASS, ComparatorType.ASCIITYPE.getClassName());
		
		cf.buildColumnDefs(columnDefs);
		cf.setUTF8Comparator();
		
		cf.createColumnFamily();
		
		KeyspaceDefinition ksd = keyspaceWrapper.getCluster().describeKeyspace(keyspaceWrapper.getKeyspaceName());
		
		List<ColumnFamilyDefinition> cfDefs = ksd.getCfDefs();
		
		for (ColumnFamilyDefinition cfDef : cfDefs) {
			String cfname = cfDef.getName();
			
			Map<String, Map<String, String>> _cfDef = cfs.get(cfname);
			
			logger.debug("cfname: " + cfname + " _cfDef: " + _cfDef);
			
			if (_cfDef != null) {
				List<ColumnDefinition> md = cfDef.getColumnMetadata();
				
				logger.debug("metadata: " + md);
				
				for (ColumnDefinition colDef : md) {
					logger.debug("coldef.name: " + _cfDef + " colDef.validationClass: " + colDef.getValidationClass());
				}
			}
		}
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}

	@Test
	public void testColumnFamilyExists() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);
		
		assertNotNull(cf);
		assertFalse(keyspaceWrapper.doesColumnFamilyExist(someCF));
		assertFalse(cf.columnFamilyExists());
		
		cf.createColumnFamily();
		
		assertTrue(cf.columnFamilyExists());
		assertTrue(keyspaceWrapper.doesColumnFamilyExist(someCF));
		
		keyspaceWrapper.dropColumnFamily(someCF);
		
		assertFalse(keyspaceWrapper.doesColumnFamilyExist(someCF));
		assertFalse(cf.columnFamilyExists());
	}
	
	@Test
	public void testSaveRowsAndColumns() throws Exception {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);

		if (cf.columnFamilyExists())
			keyspaceWrapper.dropColumnFamily(someCF);
		
		cf.createColumnFamily();
		
		Map<String, Map<String, Serializable>> values = new LinkedHashMap<String, Map<String,Serializable>>();
		
		int maxRows = 25;
		int maxCols = 25;
		
		for (int i = 0; i < maxRows; i++) {
			String key = "key_" + i;
			
			Map<String, Serializable> cols = new LinkedHashMap<String, Serializable>();
			
			values.put(key, cols);
			
			for (int j = 0; j < maxCols; j++) {
				String col = "col_" + j;
				String val = "val_" + j;
				
				cols.put(col, val);
			}
		}
		
		cf.saveRowsAndColumns(values);
		
		String endCol = "";
		String startCol = "";
		String endKey = "";
		String startKey = "";
		
		int maxCols2 = 10;
		int maxRows2 = 10;
		
		int rowsFound = 0;
		int colsFound = 0;
		
		int totalRowsFound = 0;
		int totalColsFound = 0;
		
		do {
			rowsFound = 0;
			startCol = "";
			
			Map<String, Map<String, Serializable>> rows = cf.getColumnsAsMap(startKey, endKey, startCol, endCol, maxCols2, maxRows2);
			
			for (String rowKey : rows.keySet()) {
				
				totalColsFound = 0;
				rowsFound++;
				startKey = rowKey;
				
				Map<String, Serializable> cols = rows.get(rowKey);
				
				Map<String, Serializable> _cols = values.get(rowKey);

				do {
					colsFound = 0;
					
					logger.debug(rowKey + "("+cols.size()+"/"+totalColsFound+" st: "+startCol+")");

					for (String col : cols.keySet()) {
						startCol = col;

						colsFound++;
						
						logger.debug("Found a column : " + col);
						
						if (colsFound >= maxCols2)
							break;

						_cols.remove(col);
						
						totalColsFound++;
					}
					
					if (colsFound >= maxCols2) {
						Map<String, Map<String, Serializable>> _rows = cf.getColumnsAsMap(startKey, endKey, startCol, endCol, maxCols2 + 1, maxRows2 + 1);
						cols = _rows.get(rowKey);
					}
				} while (colsFound >= maxCols2);
				
				if (_cols.size() > 0) {
					logger.debug("Remaining cols: ");
					for (String col : _cols.keySet()) {
						logger.debug("Col: " + col);
					}
				}
				
				assertEquals(0, _cols.size());
				assertEquals(maxCols, totalColsFound);
				
				logger.debug("rowsFound: " + rowsFound + " startKey: " + startKey);
				
				if (rowsFound >= maxRows2)
					break;
				
				values.remove(rowKey);
				
				totalRowsFound++;
			}
		} while (rowsFound >= maxRows2);
		
		assertEquals(maxRows, totalRowsFound);
		assertEquals(0, values.size());
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}
	
	/*
	@Test
	public void testSaveSuperRowsAndColumns() {
		String someCF = "someCF";
		
		CassandraColumnFamilyWrapper cf = keyspaceWrapper.getCassandraColumnFamilyWrapper(someCF);

		if (cf.columnFamilyExists())
			keyspaceWrapper.dropColumnFamily(someCF);
		
		cf.createColumnFamily();
		
		Map<String, Map<String, Map<String, String>>> values = new LinkedHashMap<String, Map<String, Map<String, String>>>();
		
		int maxRows = 100;
		int maxCols = 100;
		int maxSuperCols = 100;
		
		for (int i = 0; i < maxRows; i++) {
			String key = "key_" + i;
			
			Map<String, Map<String, String>> scols = new LinkedHashMap<String, Map<String,String>>();
			
			values.put(key, scols);
			
			for (int k = 0; k < maxSuperCols; k++) {
				String scolKey = "scolKey_" + k;
				
				Map<String, String> cols = new LinkedHashMap<String, String>();
				
				scols.put(scolKey, cols);
				
				for (int j = 0; j < maxCols; j++) {
					String col = "col_" + j;
					String val = "val_" + j;
					
					cols.put(col, val);
				}
			}
		}
		
		cf.saveSuperRowsAndColumns(values);
		
		String endCol = "";
		String startCol = "";
		String endKey = "";
		String startKey = "";
		
		int maxCols2 = 35;
		int maxRows2 = 35;
		
		int rowsFound = 0;
		int colsFound = 0;
		
		int totalRowsFound = 0;
		int totalColsFound = 0;
		
		do {
			rowsFound = 0;
			startCol = "";
			
			Map<String, Map<String, String>> rows = cf.getRowsWithKeyRange(startKey, endKey, startCol, endCol, maxCols2 + 1, maxRows2 + 1);
			
			for (String rowKey : rows.keySet()) {
				
				totalColsFound = 0;
				rowsFound++;
				startKey = rowKey;
				
				Map<String, String> cols = rows.get(rowKey);
				
				Map<String, String> _cols = values.get(rowKey);

				do {
					colsFound = 0;
					
					logger.debug(rowKey + "("+cols.size()+"/"+totalColsFound+" st: "+startCol+")");

					for (String col : cols.keySet()) {
						startCol = col;

						colsFound++;
						
						logger.debug("Found a column : " + col);
						
						if (colsFound >= maxCols2)
							break;

						_cols.remove(col);
						
						totalColsFound++;
					}
					
					if (colsFound >= maxCols2) {
						Map<String, Map<String, String>> _rows = cf.getRowsWithKeyRange(startKey, endKey, startCol, endCol, maxCols2 + 1, maxRows2 + 1);
						cols = _rows.get(rowKey);
					}
				} while (colsFound >= maxCols2);
				
				if (_cols.size() > 0) {
					logger.debug("Remaining cols: ");
					for (String col : _cols.keySet()) {
						logger.debug("Col: " + col);
					}
				}
				
				assertEquals(0, _cols.size());
				assertEquals(maxCols, totalColsFound);
				
				logger.debug("rowsFound: " + rowsFound + " startKey: " + startKey);
				
				if (rowsFound >= maxRows2)
					break;
				
				values.remove(rowKey);
				
				totalRowsFound++;
			}
		} while (rowsFound >= maxRows2);
		
		assertEquals(maxRows, totalRowsFound);
		assertEquals(0, values.size());
		
		keyspaceWrapper.dropColumnFamily(someCF);
	}
	*/
}
