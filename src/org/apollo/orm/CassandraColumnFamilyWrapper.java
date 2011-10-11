package org.apollo.orm;

import static me.prettyprint.hector.api.factory.HFactory.createRangeSuperSlicesQuery;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.ThriftColumnDef;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;


public class CassandraColumnFamilyWrapper {
	private Logger logger = Logger.getLogger(getClass());
	
	private Keyspace keyspace;

	private String columnFamily;
	
	private StringSerializer stringSerializer = StringSerializer.get();
	
	private ComparatorType comparatorType = ComparatorType.UTF8TYPE;

	private List<ColumnDefinition> columnMetadata = null;

	private CassandraKeyspaceWrapper keyspaceWrapper;
	
	private boolean isSuper;

	public CassandraColumnFamilyWrapper(CassandraKeyspaceWrapper keyspaceWrapper, String columnFamily, boolean isSuper) {
		this.keyspaceWrapper = keyspaceWrapper;
		this.keyspace = keyspaceWrapper.getKeyspace();
		this.columnFamily = columnFamily;
		
		this.isSuper = isSuper;
	}
	
	public CassandraColumnFamilyWrapper(CassandraKeyspaceWrapper keyspaceWrapper, String columnFamily) {
		this(keyspaceWrapper, columnFamily, false);
	}

	public void insertColumn(String key, String columnName, String value) {
		logDebug("Inserting key: '" + key + "' col: '" + columnName + "' val: '" + value + "'");
		
		if (key == null || (key != null && "".equals(key)))
			throw new RuntimeException("the parameter 'key' can neither be null or empty string");
		if (columnName == null || (columnName != null && "".equals(columnName)))
			throw new RuntimeException("the parameter 'columnName' can neither be null or empty string");
		if (value == null)
			throw new RuntimeException("the parameter 'value' can neither be null or empty string");
		
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		mutator.insert(key, columnFamily, HFactory.createStringColumn(columnName, value));
		MutationResult result = mutator.execute();
	}
	
	public void insertSuperColumn(String rowKey, String columnKey, Map<String, String> values) {
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		
		List<HColumn<String, String>> _values = new ArrayList<HColumn<String, String>>();
		
		for (String key : values.keySet()) {
			_values.add(HFactory.createStringColumn(key, values.get(key)));
		}
		
		mutator.insert(rowKey, columnFamily, HFactory.createSuperColumn(columnKey, _values, stringSerializer, stringSerializer, stringSerializer));
		
		mutator.execute();
	}
	
	public void createColumnFamily() {
		logDebug("Creating "+(isSuper ? "Super" : "Standard")+" column '" + columnFamily + "'");

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), columnFamily, comparatorType, columnMetadata);

		ThriftCfDef thriftCfDef = new ThriftCfDef(cfDef);

		if (isSuper)
			thriftCfDef.setColumnType(ColumnType.SUPER);
		
		thriftCfDef.setDefaultValidationClass(comparatorType.getClassName());
		
		keyspaceWrapper.getCluster().addColumnFamily(thriftCfDef);
		
		boolean found = false;
		long procStart = System.currentTimeMillis();
		
		do {
			KeyspaceDefinition ksd = keyspaceWrapper.getCluster().describeKeyspace(keyspace.getKeyspaceName());
			
			List<ColumnFamilyDefinition> cfs = ksd.getCfDefs();
			
			for (ColumnFamilyDefinition cf : cfs) {
				if (cf.getName().equals(columnFamily)) {
					found = true;
					break;
				}
			}
			
			if (!found) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new HectorException(e);
				}
			}
			
			if (System.currentTimeMillis() - procStart >= 10000)
				throw new HectorException("Timeout while waiting for schema to be agreed");
		}
		while (!found);
	}
	
	public void updateColumnMetadata() {
		KeyspaceDefinition ksd = keyspaceWrapper.getCluster().describeKeyspace(keyspace.getKeyspaceName());
		
		List<ColumnFamilyDefinition> cfDefs = ksd.getCfDefs();
		
		ColumnFamilyDefinition cfDef = null;
		for (ColumnFamilyDefinition _cfdef : cfDefs) {
			if (_cfdef.getName().equals(columnFamily)) {
				cfDef = _cfdef;
				break;
			}
		}
		
		if (cfDef != null && columnMetadata != null) {
			logDebug("Found existing and custom ColumnFamilyDefinition");
			
			List<ColumnDefinition> _colDef = new ArrayList<ColumnDefinition>(cfDef.getColumnMetadata());
			
			/*
			for (ColumnDefinition colDef : columnMetadata) {
				boolean found = false;
				
				for (ColumnDefinition __colDef : _colDef) {
					if (new String(colDef.getName().array()).equals(new String(__colDef.getName().array()))) {
						found = true;
						break;
					}
				}
				
				if (!found) {
					logDebug("Adding new column definition: '" + new String(colDef.getName().array()) + "'");
					
					_colDef.add(colDef);
				}
			}
			*/
			
			cfDef = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), columnFamily, comparatorType, _colDef);
			
			ColumnDef cd = new ColumnDef();
			ThriftColumnDef tcoldef = new ThriftColumnDef(cd);
			
			keyspaceWrapper.getCluster().updateColumnFamily(cfDef);
		}
	}
	
	public void updateColumnFamilyMetaData(String column, ColumnIndexType indexType, ComparatorType comparator) throws ApolloException {
		ThriftCluster cassandraCluster = keyspaceWrapper.getCluster();

		KeyspaceDefinition fromCluster = cassandraCluster.describeKeyspace(keyspace.getKeyspaceName());

		List<ColumnFamilyDefinition> cfDefs = fromCluster.getCfDefs();

		for (ColumnFamilyDefinition cfDef : cfDefs) {
			if (cfDef.getName().equals(columnFamily)) {
				
				BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition(cfDef);
				
				BasicColumnDefinition columnDefinition = new BasicColumnDefinition();

				columnDefinition.setName(StringSerializer.get().toByteBuffer(column));
				
				if (indexType == ColumnIndexType.KEYS) {
					columnDefinition.setIndexName(column);
					columnDefinition.setIndexType(indexType);
				}
				
				columnDefinition.setValidationClass(comparator.getClassName());
				
				columnFamilyDefinition.addColumnDefinition(columnDefinition);

				try {
					cassandraCluster.updateColumnFamily(new ThriftCfDef(columnFamilyDefinition));

					if (logger.isDebugEnabled())
						logger.debug("Succesfully added column index for column '" + column + "'");
				} catch (Exception e) {
					if (e.getMessage().indexOf("Duplicate index") == -1) {
						throw new ApolloException(e);
					}
				}
				
				break;
			}
		}
	}
	
	public boolean isColumnIndexed(String column) {
		ThriftCluster cassandraCluster = keyspaceWrapper.getCluster();
		
		KeyspaceDefinition fromCluster = cassandraCluster.describeKeyspace(keyspace.getKeyspaceName());
		
		List<ColumnFamilyDefinition> cfDefs = fromCluster.getCfDefs();
		
		for (ColumnFamilyDefinition cfDef : cfDefs) {
			if (cfDef.getName().equals(columnFamily)) {
				List<ColumnDefinition> colDefs = cfDef.getColumnMetadata();
				
				for (ColumnDefinition colDef : colDefs) {
					String col = StringSerializer.get().fromByteBuffer(colDef.getName());
					
					if (col.equals(column))
						return true;
				}
			}
		}
		
		return false;
	}
	
	private ColumnDef newIndexedColumnDef(String column_name, String comparer) {
		ColumnDef cd = new ColumnDef(StringSerializer.get().toByteBuffer(column_name), comparer);
		cd.setIndex_name(column_name);
		cd.setIndex_type(IndexType.KEYS);
		return cd;
	}
	
	private ColumnDef newColumnDef(String column_name, String comparer) {
		ColumnDef cd = new ColumnDef(StringSerializer.get().toByteBuffer(column_name), comparer);
		return cd;
	}
	
	public void buildColumnDefs(Map<String, Map<String, String>> columnDefs) {
		buildColumnDefs(columnDefs, false);
	}
	
	public void buildColumnDefs(Map<String, Map<String, String>> columnDefs, boolean update) {
		if (columnDefs != null) {
			List<ColumnDef> cols = null;
			
			logDebug("Building column definitions ...");
			
			for (String _columnname : columnDefs.keySet()) {
				Map<String, String> _columndefs = columnDefs.get(_columnname);
				
				String validation_class = _columndefs.get(CassandraKeyspaceWrapper.KEY_VALIDATION_CLASS);
				String key_type = _columndefs.get(CassandraKeyspaceWrapper.KEY_KEY_TYPE);
				
				if (validation_class == null || validation_class.trim().equals(""))
					validation_class = "BytesType";
				
				if (cols == null)
					cols = new ArrayList<ColumnDef>();
				
				// logDebug("Column Definition:: column_name: " + _columnname + ", validation_class: " + validation_class + ", key_type: " + key_type);
				
				ColumnDef cfDef;
				if (!isSuper & key_type != null && key_type.equalsIgnoreCase(CassandraKeyspaceWrapper.VAL_KEYS)) {
					cfDef = newIndexedColumnDef(_columnname, validation_class);
				}
				else {
					cfDef = newColumnDef(_columnname, validation_class);
				}
				
				cols.add(cfDef);
			}
			
			if (cols != null) {
				columnMetadata = ThriftColumnDef.fromThriftList(cols);
			}
		}
	}
	
	public void setASCIIComparator() {
		this.comparatorType = ComparatorType.ASCIITYPE;
	}

	public void setUTF8Comparator() {
		this.comparatorType = ComparatorType.UTF8TYPE;
	}
	
	public void setLongComparator() {
		this.comparatorType = ComparatorType.LONGTYPE;
	}
	
	public void setIntegerComparator() {
		this.comparatorType = ComparatorType.INTEGERTYPE;
	}
	
	public void setUUIDComparator() {
		this.comparatorType = ComparatorType.UUIDTYPE;
	}
	
	public void setTIMEUUIDComparator() {
		this.comparatorType = ComparatorType.TIMEUUIDTYPE;
	}
	
	public void makeKeyspace(String keyspace, List<ColumnFamilyDefinition> cf_defs)
			throws InvalidRequestException, TException {
		logDebug("Creating keyspace: " + keyspace);
		try {
			KeyspaceDefinition ks_def = HFactory.createKeyspaceDefinition(keyspace,
					"org.apache.cassandra.locator.SimpleStrategy", 1, cf_defs);

			keyspaceWrapper.getCluster().addKeyspace(ks_def);

			logDebug("Created keyspace: " + keyspace);
		} catch (Exception e) {
			logger.error("Unable to create keyspace " + keyspace, e);
		}
	}
	
	/**
	 * @param key
	 * @param columnName
	 * @return value of a column
	 */
	public String getColumnValue(String key, String columnName) {
		ColumnQuery<String, String, String> columnQuery =
			HFactory.createStringColumnQuery(keyspace);

		columnQuery.setColumnFamily(columnFamily).setKey(key).setName(columnName);

		QueryResult<HColumn<String, String>> result = columnQuery.execute();

		HColumn<String, String> col = result.get();

		if (col != null) {
			String val = col.getValue();

			return val;
		}

		return null;
	}
	
	public Map<String, Map<String, Map<String, String>>> getSuperColumns(String startKey, String endKey, 
				String[] super_column_names) {
		if (super_column_names == null || super_column_names.length == 0)
			throw new RuntimeException("Column names must be specified for types of super column family");
		
		Map<String, Map<String, Map<String, String>>> ret = null;
		
	    RangeSuperSlicesQuery<String, String, String, String> query = createRangeSuperSlicesQuery(keyspace, 
	    		stringSerializer, stringSerializer, stringSerializer, stringSerializer);
	    
	    query.setColumnFamily(columnFamily);
	    query.setKeys(startKey, endKey);
	    
	    query.setColumnNames(super_column_names);
	    
	    QueryResult<OrderedSuperRows<String, String, String, String>> result = query.execute();

	    if (result != null) {
	    	logDebug("Found a super column with start key: '" + startKey + "'");
	    	
	    	OrderedSuperRows<String, String, String, String> rows = result.get();
	    	
	    	for (SuperRow<String, String, String, String> row : rows) {
	    		Map<String, Map<String, String>> map = null;
	    		
	    		String rowKey = row.getKey();
	    		
	    		logDebug("Row key: '" + rowKey + "'");
	    		
	    		SuperSlice<String, String, String> slice = row.getSuperSlice();
	    		
	    		List<HSuperColumn<String,String,String>> cols = slice.getSuperColumns();
	    		
	    		logDebug("cols : " + cols + " size : " + (cols != null ? slice.getColumnByName(super_column_names[0]) + " (" + cols.size() + ")" : null));
	    		
	    		for (HSuperColumn<String, String, String> col : cols) {
	    			Map<String, String> map2 = null;
	    			
	    			String superColumnKey = col.getName();
	    			
	    			logDebug("Super column key '" + superColumnKey + "'");
	    			
	    			List<HColumn<String, String>> cols2 = col.getColumns();
	    			
	    			for (HColumn<String, String> col2 : cols2) {
						String _col = col2.getName();
						String _val = col2.getValue();
						
						if (map2 == null)
							map2 = new LinkedHashMap<String, String>();
						
						logDebug("Found a column: '" + _col + "' : '" + _val + "'");
						
						map2.put(_col, _val);
					}
	    			
	    			/*
	    			for (String col2 : column_names) {
	    				_col = col.get
						String _col = col2.getName();
						String _val = col2.getValue();
						
						if (map2 == null)
							map2 = new LinkedHashMap<String, String>();
						
						logger.debug("Found a column: '" + _col + "' : '" + _val + "'");
						
						map2.put(_col, _val);
					}
	    			*/
					if (map == null && map2 != null)
						map = new LinkedHashMap<String, Map<String, String>>();

					if (map2 != null)
						map.put(superColumnKey, map2);
				}
	    		
	    		if (ret == null && map != null)
	    			ret = new LinkedHashMap<String, Map<String,Map<String, String>>>();
	    		
	    		if (map != null)
	    			ret.put(rowKey, map);
			}
	    }
	    
		return ret;
	}

	public void getSuperColumns(String startKey, String endKey, 
			String[] super_column_names, int maxRows, GetSuperColumnsHandler handler) {
		if (super_column_names == null || super_column_names.length == 0)
			throw new RuntimeException("Column names must be specified for types of super column family");
		
		boolean hasHandler = handler != null;
		
		int rowsPerPage = hasHandler ? handler.getRowsPerPage() : maxRows;
		
		logDebug("createRangeSuperSlicesQuery: rowsPerPage: " + rowsPerPage + " startKey: " + startKey);
		
		RangeSuperSlicesQuery<String, String, String, String> query = createRangeSuperSlicesQuery(keyspace, 
				stringSerializer, stringSerializer, stringSerializer, stringSerializer);

		query.setColumnFamily(columnFamily);
		query.setKeys(startKey, endKey);

		query.setColumnNames(super_column_names);

		query.setRowCount(rowsPerPage);

		QueryResult<OrderedSuperRows<String, String, String, String>> result = query.execute();

		if (result != null) {
			logDebug("Found a super column with start key: '" + startKey + "'");

			OrderedSuperRows<String, String, String, String> rows = result.get();
			
			int rowsFound = 0;

			for (SuperRow<String, String, String, String> row : rows) {
				
				String rowKey = row.getKey();

				if (hasHandler) {
					handler.onStartRow(rowKey);
					
					if (!handler.scanRows())
						break;
					
					if (handler.skipRow())
						continue;
				}

				startKey = rowKey;

				//logger.debug("Row key: '" + rowKey + "'");

				if (handler == null || handler != null && handler.scanColumns()) {
					SuperSlice<String, String, String> slice = row.getSuperSlice();

					List<HSuperColumn<String,String,String>> cols = slice.getSuperColumns();

					logDebug("cols : " + cols + " size : " + (cols != null ? slice.getColumnByName(super_column_names[0]) + " (" + cols.size() + ")" : null));

					for (HSuperColumn<String, String, String> col : cols) {
						String superColumnKey = col.getName();

						if (hasHandler)
							handler.onStartSuperColumn(superColumnKey);

						//logger.debug("Super column key '" + superColumnKey + "'");

						List<HColumn<String, String>> cols2 = col.getColumns();

						for (HColumn<String, String> col2 : cols2) {
							String _col = col2.getName();
							String _val = col2.getValue();

							//logger.debug("Found a column: '" + _col + "' : '" + _val + "'");

							if (hasHandler)
								handler.onColumn(_col, _val);
						}

						if (hasHandler)
							handler.onEndSuperColumn(superColumnKey);
					}
				}

				if (hasHandler)
					handler.onEndRow(rowKey);

				
				rowsFound++;
				
				if (rowsFound > rowsPerPage)
					break;
			}
		}
	}
	
	/**
	 * @param key
	 * @param columnNames
	 * @return null if there aren't any results
	 */
	public Map<String, String> getMultipleColumns(String key, String[] columnNames) {
		SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);

		q.setColumnFamily(columnFamily)
		.setKey(key)
		.setColumnNames(columnNames);

		QueryResult<ColumnSlice<String, String>> r = q.execute();

		ColumnSlice<String, String> cs = r.get();

		List<HColumn<String, String>> cols = cs.getColumns();

		Map<String, String> ret = null;

		for (Iterator<HColumn<String, String>> iterator = cols.iterator(); iterator.hasNext();) {
			HColumn<String, String> hcol = (HColumn<String, String>) iterator.next();

			String col = hcol.getName();
			String val = hcol.getValue();

			if (ret == null)
				ret = new LinkedHashMap<String, String>();

			ret.put(col, val);
		}

		return ret;
	}
	
	public Map<String, Map<String, String>> findColumnWithValue(List<Expression> criterias, Session session, 
			ClassConfig classConfig, ArrayList<String> column_list) 
					throws SecurityException, IllegalArgumentException, NoSuchMethodException, IllegalAccessException, 
							InvocationTargetException {
		
		Map<String, Map<String, String>> ret = null;

		IndexedSlicesQuery<String, String, String> q = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
		
		q.setColumnFamily(columnFamily);
		
		List<Serializable> startKey = null;
		
		for (Expression exp : criterias) {
			String prop = exp.getProperty();
			
			if (prop.equalsIgnoreCase(classConfig.idMethod)) {
				if (startKey == null)
					startKey = new ArrayList<Serializable>();
				
				if (exp.getOperation().equals(Expression.OPERATION_IN))
					startKey.addAll((Collection<? extends Serializable>) exp.getExpected());
				else
					startKey.add(exp.getExpected().toString());
				
				continue;
			}
			
			boolean special = prop.startsWith("__") && prop.endsWith("__");
			
			Map<String, String> methodConfig = classConfig.getMethodConfig(prop);
			
			if (methodConfig == null && !special)
				throw new IllegalArgumentException("Uknown property '" + prop + "' for class " + classConfig.clazz);
			
			String column = special ? prop : methodConfig.get("column");
			
			String expected = null;
			
			if (!special) {
				if (methodConfig == null)
					throw new IllegalArgumentException("Undefined property '" + prop + "' for class '" + classConfig.clazz + "'");
			
				String child_table_name = methodConfig.get("table");
				
				Class propType = classConfig.getPropertyType(prop);
				
				if (propType == Integer.TYPE
						|| propType == Long.TYPE
						|| propType == Short.TYPE
						|| propType == Double.TYPE
						|| propType == Float.TYPE
						|| propType == Byte.TYPE
						|| propType == String.class
						) {
					expected = exp.getExpected().toString();
				}
				else if (propType == Timestamp.class) {
					// TODO
					throw new IllegalArgumentException("feature not implemented yet");
				}
				else if (propType == List.class) {
					// TODO
					throw new IllegalArgumentException("feature not implemented yet");
				}
				else if (propType == Map.class) {
					// TODO
					throw new IllegalArgumentException("feature not implemented yet");
				}
				else if (propType == Set.class) {
					// TODO
					throw new IllegalArgumentException("feature not implemented yet");
				}
				else {
					ClassConfig child_classConfig = ((SessionImpl) session).getClassConfig(propType, false);
					
					if (child_classConfig != null) {
						Serializable idValue = child_classConfig.getIdValue(exp.getExpected());
						
						expected = idValue.toString();
					}
					else {
						throw new IllegalArgumentException("Unhandled condition");
					}
				}
			}
			else {
				expected = exp.getExpected().toString();
			}
			
			if (logger.isDebugEnabled())
				logDebug("prop: "+prop+" col: " + column + " exp: " + expected + " opr: " + exp.getOperation());
			
			if (exp.getOperation().equals(Expression.OPERATION_EQ))
				q.addEqualsExpression(column, expected);
			else if (exp.getOperation().equals(Expression.OPERATION_GT))
				q.addGtExpression(column, expected);
			else if (exp.getOperation().equals(Expression.OPERATION_GTE))
				q.addGteExpression(column, expected);
			else if (exp.getOperation().equals(Expression.OPERATION_LT))
				q.addLtExpression(column, expected);
			else if (exp.getOperation().equals(Expression.OPERATION_LTE))
				q.addLteExpression(column, expected);
		}
		
		if (logger.isDebugEnabled()) {
			logDebug("column_names: " + column_list);
			
			if (column_list != null) {
				for (String column : column_list) {
					logDebug("column: " + column);
				}
			}
		}
		
		if (startKey != null && startKey.size() == 1)
			q.setStartKey(startKey.get(0).toString());
		
		if (logger.isDebugEnabled()) {
			if (startKey != null && startKey.size() > 1)
				logDebug("Will be performing selected keys only");
		}
		
		q.setColumnNames(column_list);
		
		QueryResult<OrderedRows<String, String, String>> r = q.execute();
		
		OrderedRows<String, String, String> _rows = r.get();
		
		List<Row<String, String, String>> rows = _rows.getList();
		
		for (Row<String, String, String> row : rows) {
			if (logger.isDebugEnabled())
				logDebug("Found a Row: " + row.getKey());
			
			if (startKey != null && startKey.size() > 1 && !startKey.contains(row.getKey())) {
				if (logger.isDebugEnabled())
					logDebug("Skipping key: " + row.getKey());
				
				continue;
			}
			
			if (ret == null)
				ret = new LinkedHashMap<String, Map<String,String>>();
			
			List<HColumn<String, String>> cols = row.getColumnSlice().getColumns();
			
			if (cols != null) {
				Map<String, String> rcols = new LinkedHashMap<String, String>();
				
				for (HColumn<String, String> hcol : cols) {
					rcols.put(hcol.getName(), hcol.getValue());
				}
				
				ret.put(row.getKey(), rcols);
			}
		}
		
		return ret;
	}
	
	public Map<String, Map<String, String>> getRowsWithMultipleKeys(String[] keys, String columnStart, String columnEnd, int maxColumCount) {
		Map<String, Map<String, String>> ret = null;

		MultigetSliceQuery<String, String, String> multigetSliceQuery =
			HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);

		multigetSliceQuery.setColumnFamily(columnFamily);

		multigetSliceQuery.setKeys(keys);
		
		multigetSliceQuery.setRange(columnStart, columnEnd, false, maxColumCount);

		QueryResult<Rows<String, String, String>> result = multigetSliceQuery.execute();

		Rows<String, String, String> rows = result.get();

		for (Row<String, String, String> row : rows) {
			ColumnSlice<String, String> cs = row.getColumnSlice();

			List<HColumn<String, String>> cols = cs.getColumns();

			Iterator<HColumn<String, String>> iterator = cols.iterator();

			HashMap<String, String> map = null;

			String key = row.getKey();

			for (; iterator.hasNext();) {
				HColumn<String, String> hcol = (HColumn<String, String>) iterator.next();

				String col = hcol.getName();
				String val = hcol.getValue();

				if (map == null)
					map = new LinkedHashMap<String, String>();

				map.put(col, val);
			}

			if (map != null && ret == null)
				ret = new LinkedHashMap<String, Map<String,String>>();

			if (map != null)
				ret.put(key, map);
		}

		return ret;
	}

	public Map<String, Map<String, String>> getRowsWithMultipleKeys(String[] keys) {
		return getRowsWithMultipleKeys(keys, "", "", 99);
	}

	public void deleteRow(String key) {
		logDebug("Deleting row with key '" + key + "'");
		
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		mutator.delete(key, columnFamily, null, stringSerializer);
		mutator.execute();
	}
	
	public void deleteColumn(String key, String column) {
		//logDebug("Deleting column key: '" + key + "' col: '" + column + "'");
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		mutator.delete(key, columnFamily, column, stringSerializer);
		mutator.execute();
	}
	
	public Map<String, Map<String, String>> getColumnsAsMap(final String startKey, String endKey, final String startColumn, 
			String endColumn, final int maxColumns, final int maxRows) {
		return getColumnsAsMap(startKey, endKey, startColumn, endColumn, maxColumns, maxRows, maxRows > ApolloConstants.MAX_ROWS_PER_PAGE ? ApolloConstants.MAX_ROWS_PER_PAGE : maxRows);
	}
	
	public Map<String, Map<String, String>> getColumnsAsMap(final String startKey, String endKey, final String startColumn, 
			String endColumn, final int maxColumns, final int maxRows, boolean keysOnly) {
		return getColumnsAsMap(startKey, endKey, startColumn, endColumn, maxColumns, maxRows, maxRows > 100 || maxRows == 0 ? 100 : maxRows, keysOnly);
	}
	
	public Map<String, Map<String, String>> getColumnsAsMap(final String startKey, String endKey, final String startColumn, 
			String endColumn, final int maxColumns, final int maxRows, final int rowsPerPage) {
		return getColumnsAsMap(startKey, endKey, startColumn, endColumn, maxColumns, maxRows, rowsPerPage, false);
	}
		
	public Map<String, Map<String, String>> getColumnsAsMap(final String startKey, String endKey, final String startColumn, 
			String endColumn, final int maxColumns, final int maxRows, final int rowsPerPage, boolean keysOnly) {
			
		final Map<String, Map<String, String>> ret = new LinkedHashMap<String, Map<String,String>>();
		
		if (logger.isDebugEnabled())
			logDebug("Retrieving colums as map: startKey: '" + startKey + "' startColumn: '" + startColumn + "' maxRows: " + maxRows + " rowsPerPage: " + rowsPerPage + " maxCols: " + maxColumns);
		
		class Counter {
			int tmpRowCount;
			
			int tmpColCount;
			
			String _startKey = startKey;
			
			String _startCol = startColumn;

			int _maxRows = rowsPerPage > 1 ? rowsPerPage + 1 : 1;

			int totalRows;
		}
		
		final Counter c = new Counter();
		
		final Map<String, String> rowKeyLastCol = new LinkedHashMap<String, String>();
		
		do {
			if (rowKeyLastCol.size() > 0) {
				c._startKey = rowKeyLastCol.keySet().iterator().next();
				c._startCol = rowKeyLastCol.get(c._startKey);
				c._maxRows = 1;
				
				rowKeyLastCol.remove(c._startKey);
				
				logDebug("Searching for more columns for rowKey: " + c._startKey);
			}
			
			c.totalRows = 0;
			
			do {
				c.tmpRowCount = 0;

				getColumns(c._startKey, endKey, c._startCol, endColumn, maxColumns, c._maxRows, keysOnly, new GetColumnsHandlerAdapter() {
					private int skipRows;
					private int skipCols;
					private Map<String, String> cols;

					@Override
					public void onStartRow(String rowKey) {
						c._startKey = rowKey;

						c.tmpRowCount++;

						c.tmpColCount = 0;

						if (c.tmpRowCount > rowsPerPage) {
							skipRows = 1;
							return;
						}
						
						c.totalRows++;
						
						cols = ret.get(rowKey);
						
						if (cols == null) {
							cols = new LinkedHashMap<String, String>();
							
							ret.put(rowKey, cols);
						}
						
						assertNotNull(cols);

						if (logger.isDebugEnabled())
							log("START ROW: RowKey: " + rowKey + " tmpRowCount: " + c.tmpRowCount);
					}

					@Override
					public void onColumn(String col, String val) {
						c.tmpColCount++;

						//c.startCol = col;

						if (maxColumns > 1 && c.tmpColCount > maxColumns) {
							skipCols = 1;

							rowKeyLastCol.put(c._startKey, col);

							if (logger.isDebugEnabled())
								logDebug("Row Key: " + c._startKey + " col: " + col + " tmpColCount: " + c.tmpColCount + ". Skipping.");

							return;
						}

						cols.put(col, val);
						
						if (logger.isDebugEnabled())
							log("Row Key: " + c._startKey + " col: " + col + " tmpColCount: " + c.tmpColCount);
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
					public boolean skipCol() {
						if (skipCols > 0) {
							skipCols--;
							return true;
						}

						return super.skipCol();
					}

					void log(String msg) {
						logDebug("Custom Handler: " + msg);
					}
				});
				
			} while (c.tmpRowCount >= rowsPerPage && (maxRows == 0 || (maxRows > 1 && c.totalRows < maxRows)));
			
		} while (rowKeyLastCol.size() > 0);
		
		if (logger.isDebugEnabled())
			logDebug("Returning: " + (ret != null ? ret.size() + " records " : "") + "contents: " + ret);
		
		return ret.size() > 0 ? ret : null;
	}
	
	public void getColumns(String startKey, String endKey, String startColumn, 
			String endColumn, int maxColumns, int maxRows, GetColumnsHandler handler) {
		getColumns(startKey, endKey, startColumn, endColumn, maxColumns, maxRows, false, handler);
	}
	
	public void getColumns(String startKey, String endKey, String startColumn, 
			String endColumn, int maxColumns, int maxRows, boolean keysOnly, GetColumnsHandler handler) {
		
		//logDebug("getRowsWithKeyRange: START");
		
		RangeSlicesQuery<String, String, String> rangeSlicesQuery
				= HFactory.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);

		rangeSlicesQuery.setColumnFamily(columnFamily);
		rangeSlicesQuery.setKeys(startKey, endKey);
		rangeSlicesQuery.setRange(startColumn, endColumn, false, maxColumns);
		
		rangeSlicesQuery.setRowCount(maxRows); 
		
		if (keysOnly)
			rangeSlicesQuery.setReturnKeysOnly();

		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();

		OrderedRows<String, String, String> orows = result.get();

		/*
		logDebug("About to do range scan. startKey: '" + startKey + "' startColumn: '" + startColumn 
				+ "' maxRows: " + maxRows + " maxColumns: " + maxColumns + " result count: " + orows.getCount());
		*/
		
		for (Row<String, String, String> row : orows) {
			String key = row.getKey();

			startKey = key;
			
			// logDebug("key: " + key);

			handler.onStartRow(key);

			if (!handler.scanRows())
				break;

			if (handler.skipRow())
				continue;

			if (!keysOnly) {
				ColumnSlice<String, String> cs = row.getColumnSlice(); 

				int colsCounted = 0;

				List<HColumn<String, String>> cols = cs.getColumns();

				//logger.debug("cols.size: " + cols.size());

				Iterator<HColumn<String, String>> iterator = cols.iterator();

				for (; iterator.hasNext();) {
					HColumn<String, String> hcol = (HColumn<String, String>) iterator.next();

					String col = hcol.getName();
					String val = hcol.getValue();

					handler.onColumn(col, val);

					if (!handler.scanCols())
						break;
					if (handler.skipCol())
						continue;

					colsCounted++;

					//logger.debug("RowKey: " + key + " colsCounted: " + colsCounted + " lastCol: '" + lastCol + "'");

					if (colsCounted >= maxColumns)
						break;
				}
			}

			handler.onEndRow(key);
		}
		
		// logDebug("getRowsWithKeyRange: END");
	}

	public boolean isSuper() {
		return isSuper;
	}

	public String getColumnFamilyName() {
		return columnFamily;
	}

	public ComparatorType getComparator() {
		return comparatorType;
	}

	public String getSuperColumnValue(String rowKey, String columnKey, String column) {
	    RangeSuperSlicesQuery<String, String, String, String> query = createRangeSuperSlicesQuery(keyspace, 
	    		stringSerializer, stringSerializer, stringSerializer, stringSerializer);
	    
	    query.setColumnFamily(columnFamily);
	    
	    query.setKeys(rowKey, "");
	    
	    query.setColumnNames(new String[] {columnKey});
	    
	    QueryResult<OrderedSuperRows<String, String, String, String>> result = query.execute();

	    if (result != null) {
	    	OrderedSuperRows<String, String, String, String> rows = result.get();
	    	
	    	SuperRow<String, String, String, String> row = rows.getByKey(rowKey);
	    	
	    	if (row != null && row.getKey().equals(rowKey)) {
	    		//logger.debug("Found SuperColumn with row key: " + rowKey);

	    		SuperSlice<String, String, String> slice = row.getSuperSlice();
	    		
	    		HSuperColumn<String, String, String> col = slice.getColumnByName(columnKey);

	    		if (col != null) {
	    			//logger.debug("Found SuperColumn with row key: " + rowKey + " and column key : " + columnKey);

	    			List<HColumn<String, String>> cols2 = col.getColumns();

	    			for (HColumn<String, String> col2 : cols2) {
	    				String _col = col2.getName();
	    				String _val = col2.getValue();

	    				if (_col.equals(column))
	    					return _val;
	    			}
	    		}
			}
	    }
	    
		return null;
	}
	
	public boolean columnFamilyExists() {
		return keyspaceWrapper.doesColumnFamilyExist(columnFamily);
	}

	public void saveRowsAndColumns(Map<String, Map<String, String>> rows) {
		if (isSuper)
			throw new RuntimeException("saveRowsAndColumns is only for standard column families");
		
		logDebug("Trying to save : " + rows);
		
		for (String rowKey : rows.keySet()) {
			Map<String, String> cols = rows.get(rowKey);
			
			for (String col : cols.keySet()) {
				String val = cols.get(col);
				
				logDebug("Saving key: " + rowKey + " col: " + col + " val: " + val);
				
				insertColumn(rowKey, col, val);
			}
		}
	}
	
	public void saveSuperRowsAndColumns(Map<String, Map<String, Map<String, String>>> rows) {
		if (!isSuper)
			throw new RuntimeException("saveRowsAndColumns is only for standard column families");
		
		for (String rowKey : rows.keySet()) {
			logDebug("RowKey: " + rowKey);
			
			Map<String, Map<String, String>> scols = rows.get(rowKey);
			
			for (String scol : scols.keySet()) {
				Map<String, String> cols = scols.get(scol);
				
				insertSuperColumn(rowKey, scol, cols);
			}
		}
	}
	
	public void truncate() {
		keyspaceWrapper.truncateColumnFamily(columnFamily);
	}
	
	void logDebug(String msg) {
		logger.debug("CFW[" + columnFamily + "] " + msg);
	}
}
