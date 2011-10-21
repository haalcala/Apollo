package org.apollo.orm;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;


public interface ApolloConstants {

	public static final int MAX_COLUMN_PER_PAGE = 100;
	
	public static final int MAX_ROWS_PER_PAGE = 100;

	public static final String NODE_MANY_TO_MANY = "many-to-many";
	public static final String NODE_SET = "set";
	public static final String NODE_ONE_TO_ONE = "one-to-one";
	public static final String NODE_MANY_TO_ONE = "many-to-one";
	public static final String NODE_PROPERTY = "property";
	public static final String NODE_HIBERNATE_MAPPING = "hibernate-mapping";
	public static final String NODE_ID = "id";

	public static final String ATTR_GENERATOR = "generator";
	public static final String ATTR_COLUMN = "column";
	public static final String ATTR_LAZY_LOADED = "lazy-loaded";
	public static final String ATTR_NAME = "name";
	public static final String ATTR_CLASS = "class";
	public static final String ATTR_TABLE = "table";
	public static final String ATTR_UNSAVED_VALUE = "unsaved-value";
	public static final String ATTR_NOT_NULL = "not-null";
	public static final String ATTR_CASCADE = "cascade";
	public static final String ATTR_CHILD_TABLE_KEY_SUFFIX = "child-table-key-suffix";

	
	public static final String STR_NATIVE = "native";
	public static final String STR_NULL = "null";

	public static final String STR_CLUSTER_SCHEMA_DOES_NOT_YET_AGREE = "Cluster schema does not yet agree";

	public static final String METHOD_PREFIX_SET = "set";
	public static final String METHOD_PREFIX_GET = "get";
	public static final String METHOD_PREFIX_IS = "is";

	public static final String SYS_COL_RSTAT = "__rstat__";
	
	public static final String SYS_APOLLO_SYMBOL_PREFIX = "apollo://";
	
	public static final String SYS_STR_MAP_KEY_PREFIX = "map-key-prefix";
	public static final String SYS_STR_SET_KEY_INDEX = "set-key-index";
	
	public static final String SYSTEM_DATE_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS";


	public static class Util {
		public static ApolloIterator<String> getApolloRowIterator(CassandraColumnFamilyWrapper cf) {
			return new ApolloKeyIteratorImpl(cf, "", 0, null, null, 0, MAX_ROWS_PER_PAGE, true);
		}
		
		public static ApolloIterator<String> getApolloRowIterator(CassandraColumnFamilyWrapper cf, String startKey) {
			return new ApolloKeyIteratorImpl(cf, startKey, 0, null, null, 0, MAX_ROWS_PER_PAGE, true);
		}
		
		public static ApolloIterator<String> getApolloRowIterator(CassandraColumnFamilyWrapper cf, String startKey, int rowsPerPage) {
			return new ApolloKeyIteratorImpl(cf, startKey, 0, null, null, 0, rowsPerPage, true);
		}
		
		public static ApolloIterator<String> getApolloColumnIterator(CassandraColumnFamilyWrapper cf, String rowKey) {
			return new ApolloKeyIteratorImpl(cf, rowKey, 0, "", "", 0, MAX_COLUMN_PER_PAGE, false);
		}
		
		public static ApolloIterator<String> getApolloColumnIterator(CassandraColumnFamilyWrapper cf, String rowKey, String startCol, String endCol, int maxColumns) {
			return new ApolloKeyIteratorImpl(cf, rowKey, 0, startCol, endCol, maxColumns, MAX_COLUMN_PER_PAGE, false);
		}
		
		public static ApolloIterator<String> getApolloColumnIterator(CassandraColumnFamilyWrapper cf, String rowKey, String startCol, String endCol, int maxColumns, int colsPerPage) {
			return new ApolloKeyIteratorImpl(cf, rowKey, 0, startCol, endCol, maxColumns, colsPerPage, false);
		}
		
		public static boolean isNativelySupported(Class<?> c) {
			if (c == Integer.TYPE
					|| c == Long.TYPE
					|| c == Boolean.TYPE
					|| c == Double.TYPE
					|| c == Byte.TYPE
					|| c == Float.TYPE
					|| c == String.class
					) {
				return true;
			}
			
			return false;
		}

		public static String getObjectValue(Object o, SessionImpl session) throws Exception {
			if (o == null)
				throw new NullPointerException();
			
			Class<?> c = o.getClass();
			
			if (isNativelySupported(c)) {
				return o.toString();
			}
			else {
				ClassConfig cc = session.getClassConfig(c);
				
				return (String) cc.getIdValue(o);
			}
		}

		public static boolean getBooleanValue(String string, boolean b) {
			return string != null ? Boolean.parseBoolean(string) : b;
		}
		
		public static <T> Class getSubType(Set<T> set) {
			for (Method m : set.getClass().getMethods()) {
				if (m.getName().equals("add")) {
					return m.getParameterTypes()[0];
				}
			}
			
			return null;
		}

		public static Timestamp getTimestamp(String val) throws ParseException {
			SimpleDateFormat sdf = new SimpleDateFormat(SYSTEM_DATE_FORMAT);

			Timestamp ret = new Timestamp(sdf.parse((String) val).getTime());
			
			return ret;
		}

		public static String getString(Timestamp timestamp) {
			SimpleDateFormat sdf = new SimpleDateFormat(SYSTEM_DATE_FORMAT);
			
			return sdf.format(timestamp);
		}
		
		public static String getMapKey(String rowKey, String prop) {
			return getMapKey(rowKey, prop, null);
		}

		public static String getMapKey(String rowKey, String prop, String child_table_key_suffix) {
			return SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_MAP_KEY_PREFIX + "[" + rowKey + "|" + prop + (child_table_key_suffix != null ? "|" + child_table_key_suffix : "") + "]";
		}
		
		public static String getSetKey(String idValue, String prop) {
			return getSetKey(idValue, prop, null);
		}

		public static String getSetKey(String idValue, String prop, String child_table_key_suffix) {
			return SYS_APOLLO_SYMBOL_PREFIX + SYS_STR_SET_KEY_INDEX + "[" + idValue + "|" + prop + (child_table_key_suffix != null ? "|" + child_table_key_suffix : "") + "]";
		}
	}
}
