package org.apollo.orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.apache.log4j.Logger;
import org.apollo.orm.beans.MyBean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Harold Alcala
 * asdfasdfsadf
 */
public class SessionFactoryImpl_Functionality_Test {
	static Logger logger = Logger.getLogger(SessionFactoryImpl_Functionality_Test.class);
	
	Configurator configurator;
	SessionFactory factory;
	Session session;

	Properties conf = new Properties();
	
	String path_bean_xml;
	
	public SessionFactoryImpl_Functionality_Test() throws Exception {
		conf.load(ClassLoader.getSystemResourceAsStream("apollo.conf"));
	}

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
		if (session != null) {
			session.close();
			session = null;
		}
		
		if (factory != null) {
			factory.shutdown();
			factory = null;
		}
	}
	
	void configure() throws Exception {
		configure(false);
	}
	
	void configure(boolean b) throws Exception {
		if (session != null) {
			session.close();
			session = null;
		}
		
		if (factory != null) {
			factory.shutdown();
			factory = null;
		}
		
		configurator = new Configurator(conf);
		
		if (path_bean_xml != null && !path_bean_xml.startsWith("org/apollo/orm/beans/"))
			path_bean_xml = "org/apollo/orm/beans/" + path_bean_xml;
		
		configurator.addClassConfiguration(path_bean_xml);
		
		factory = configurator.configure();
		
		session = factory.getSession();
		
		if (b)
			session.truncate(MyBean.class);
	}

	@Test
	public void testIntProperties() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_int_prop.hbm.xml";

		List<Integer> values = new ArrayList<Integer>();

		values.add(0);
		values.add(Integer.MIN_VALUE);
		values.add(Integer.MAX_VALUE);

		testProp("intProp", Integer.TYPE, values);
	}

	@Test
	public void testLongProperties() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_long_prop.hbm.xml";

		List<Long> values = new ArrayList<Long>();

		values.add(0L);
		values.add(Long.MIN_VALUE);
		values.add(Long.MAX_VALUE);

		testProp("longProp", Long.TYPE, values);
	}

	@Test
	public void testDoubleProperties() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_double_prop.hbm.xml";

		List<Double> values = new ArrayList<Double>();

		values.add(0D);
		values.add(Double.MIN_VALUE);
		values.add(Double.MAX_VALUE);

		testProp("doubleProp", Double.TYPE, values);
	}

	@Test
	public void testFloatProperties() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_float_prop.hbm.xml";

		List<Float> values = new ArrayList<Float>();

		values.add(0F);
		values.add(Float.MIN_VALUE);
		values.add(Float.MAX_VALUE);

		testProp("floatProp", Float.TYPE, values);
	}

	@Test
	public void testByteProperties() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_byte_prop.hbm.xml";

		List<Byte> values = new ArrayList<Byte>();

		values.add((byte) 0);
		values.add(Byte.MIN_VALUE);
		values.add(Byte.MAX_VALUE);

		testProp("byteProp", Byte.TYPE, values);
	}

	@Test
	public void testStringProperties() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_string_prop.hbm.xml";

		List<String> values = new ArrayList<String>();

		values.add(null);
		values.add("The quick brown fox jumps over the lazy dog.");
		values.add("Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  ");

		testProp("stringProp", String.class, values);
	}

	@Test
	public void testTimestampProperties() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_timestamp_prop.hbm.xml";

		long ctm = System.currentTimeMillis();

		List<Timestamp> values = new ArrayList<Timestamp>();

		values.add(null);
		values.add(new Timestamp(0));
		values.add(new Timestamp((ctm % 1000) * 1000));

		testProp("timestampProp", Timestamp.class, values);
	}
	
	@Test
	public void testListProp() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_list_prop.hbm.xml";
		
		List<String> values = new ArrayList<String>();

		values.add("a");
		values.add("The quick brown fox jumps over the lazy dog.");
		values.add("Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  ");

		configure(true);
		
		MyBean bean = new MyBean();
		
		bean.listProp = new ArrayList<String>(values);
		
		session.save(bean);
		
		assertNotNull(bean.getId());
		assertNotNull(bean.id);
		
		configure();
		
		String idValue = bean.id;
		
		MyBean bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		assertNotNull(bean2.listProp);
		
		for (String str : values) {
			assertTrue(bean2.listProp.contains(str));
		}
	}

	@Test
	public void testMapProp() throws Exception {
		path_bean_xml = "MyBean_with_native_map_prop.hbm.xml";
		
		configure(true);
		
		MyBean bean = new MyBean();
		
		bean.mapProp = new HashMap<String, String>();
		
		String[] keys = {"a", "b", "c", "1", "2", "3"};
		String[] vals = {"1", "2", "3", "a", "b", "c"};
		
		for (int i = 0; i < vals.length; i++) {
			bean.mapProp.put(keys[i], vals[i]);
		}
		
		session.save(bean);
		
		assertNotNull(bean.getId());
		assertNotNull(bean.id);
		
		for (int i = 0; i < vals.length; i++) {
			assertEquals(vals[i], bean.mapProp.get(keys[i]));
		}
		
		configure();
		
		String idValue = bean.id;
		
		MyBean bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		assertNotNull(bean2.mapProp);
		
		assertEquals(idValue, bean2.id);
		assertEquals(idValue, bean2.getId());
		
		for (int i = 0; i < vals.length; i++) {
			assertEquals(vals[i], bean2.mapProp.get(keys[i]));
		}
	}
	
	@Test
	public void testMapProp_LazyLoaded() throws Exception {
		path_bean_xml = "MyBean_with_native_map_prop_lazy_loaded.hbm.xml";
		
		configure(true);
		
		MyBean bean = new MyBean();
		
		bean.mapProp = new HashMap<String, String>();
		
		String[] keys = {"a", "b", "c", "1", "2", "3"};
		String[] vals = {"1", "2", "3", "a", "b", "c"};
		
		for (int i = 0; i < vals.length; i++) {
			bean.mapProp.put(keys[i], vals[i]);
		}
		
		session.save(bean);
		
		assertNotNull(bean.getId());
		assertNotNull(bean.id);
		
		for (int i = 0; i < vals.length; i++) {
			assertEquals(vals[i], bean.mapProp.get(keys[i]));
		}
		
		configure();
		
		String idValue = bean.id;
		
		MyBean bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		assertNotNull(bean2.mapProp);
		
		assertEquals(idValue, bean2.id);
		assertEquals(idValue, bean2.getId());
		
		for (int i = 0; i < vals.length; i++) {
			assertEquals(vals[i], bean2.mapProp.get(keys[i]));
		}
	}
	
	@Test
	public void testIntPropertiesWithColumn() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_int_prop_with_column.hbm.xml";
		
		List<Integer> values = new ArrayList<Integer>();
		
		values.add(0);
		values.add(Integer.MIN_VALUE);
		values.add(Integer.MAX_VALUE);
		
		testProp("intPropWithColumn", Integer.TYPE, values);
	}
	
	@Test
	public void testLongPropertiesWithColumn() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_long_prop_with_column.hbm.xml";
		
		List<Long> values = new ArrayList<Long>();
		
		values.add(0L);
		values.add(Long.MIN_VALUE);
		values.add(Long.MAX_VALUE);
		
		testProp("longPropWithColumn", Long.TYPE, values);
	}
	
	@Test
	public void testDoublePropertiesWithColumn() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_double_prop_with_column.hbm.xml";
		
		List<Double> values = new ArrayList<Double>();
		
		values.add(0D);
		values.add(Double.MIN_VALUE);
		values.add(Double.MAX_VALUE);
		
		testProp("doublePropWithColumn", Double.TYPE, values);
	}
	
	@Test
	public void testFloatPropertiesWithColumn() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_float_prop_with_column.hbm.xml";
		
		List<Float> values = new ArrayList<Float>();
		
		values.add(0F);
		values.add(Float.MIN_VALUE);
		values.add(Float.MAX_VALUE);
		
		testProp("floatPropWithColumn", Float.TYPE, values);
	}
	
	@Test
	public void testBytePropertiesWithColumn() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_byte_prop_with_column.hbm.xml";
		
		List<Byte> values = new ArrayList<Byte>();
		
		values.add((byte) 0);
		values.add(Byte.MIN_VALUE);
		values.add(Byte.MAX_VALUE);
		
		testProp("bytePropWithColumn", Byte.TYPE, values);
	}
	
	@Test
	public void testStringPropertiesWithColumn() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_string_prop_with_column.hbm.xml";
		
		List<String> values = new ArrayList<String>();
		
		values.add(null);
		values.add("The quick brown fox jumps over the lazy dog.");
		values.add("Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  " +
				"Extremely long string.  Extremely long string.  Extremely long string.  Extremely long string.  ");
		
		testProp("stringPropWithColumn", String.class, values);
	}
	
	@Test
	public void testTimestampPropertiesWithColumn() throws Exception {
		path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_timestamp_prop_with_column.hbm.xml";
		
		long ctm = System.currentTimeMillis();
		
		List<Timestamp> values = new ArrayList<Timestamp>();
		
		values.add(null);
		values.add(new Timestamp(0));
		values.add(new Timestamp((ctm % 1000) * 1000));
		
		testProp("timestampPropWithColumn", Timestamp.class, values);
	}
	
	@Test
	public void testMapPropWithColumn() throws Exception {
		path_bean_xml = "MyBean_with_native_map_prop.hbm.xml";
		
		configure(true);
		
		MyBean bean = new MyBean();
		
		bean.mapProp = new HashMap<String, String>();
		
		String[] keys = {"a", "b", "c", "1", "2", "3"};
		String[] vals = {"1", "2", "3", "a", "b", "c"};
		
		for (int i = 0; i < vals.length; i++) {
			bean.mapProp.put(keys[i], vals[i]);
		}
		
		session.save(bean);
		
		assertNotNull(bean.getId());
		assertNotNull(bean.id);
		
		for (int i = 0; i < vals.length; i++) {
			assertEquals(vals[i], bean.mapProp.get(keys[i]));
		}
		
		configure();
		
		String idValue = bean.id;
		
		MyBean bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		assertNotNull(bean2.mapProp);
		
		assertEquals(idValue, bean2.id);
		assertEquals(idValue, bean2.getId());
		
		for (int i = 0; i < vals.length; i++) {
			assertEquals(vals[i], bean2.mapProp.get(keys[i]));
		}
	}
	
	void testProp(String prop, Class<?> clazz, List<?> values) throws Exception {
		MyBean bean;
		
		for (Object value : values) {
			logger.debug("###########################################################################################");
			logger.debug("#### Testing Data Storage for " + clazz + " property: " + prop + " value: " + value);
			
			configure(true);
			
			bean = new MyBean();

			String getMethodName = clazz == Boolean.TYPE ?  SessionImpl.getIsMethodFromProperty(prop) : SessionImpl.getGetMethodFromProperty(prop);
			
			String setMethodName = SessionImpl.getSetMethodFromProperty(prop);
			
			Method getMethod = MyBean.class.getMethod(getMethodName, null);
			
			Method setMethod = MyBean.class.getMethod(setMethodName, new Class<?>[] {clazz});

			setMethod.invoke(bean, value);

			session.save(bean);

			assertNotNull(bean.getId());
			assertNotNull(bean.id);
			assertEquals(value, getMethod.invoke(bean, null));
			
			configure();
			
			String idValue = bean.id;
			
			logger.debug("#### Testing Data Retrieval for " + clazz + " property: " + prop + " value: " + value);
			
			MyBean bean2 = session.find(MyBean.class, idValue);
			
			assertNotSame(bean, bean2);
			
			assertNotNull(bean2);
			
			assertEquals(idValue, bean2.id);
			assertEquals(idValue, bean2.getId());
				
			assertEquals(value, getMethod.invoke(bean2, null));
		}
	}
	
	@Test
	public void testSetProprety_Simple() throws Exception {
		path_bean_xml = "MyBean_with_native_set_prop.hbm.xml";
		
		configure(true);
		
		MyBean bean = new MyBean();
		
		bean.setSetStringProp = new HashSet<String>();
		
		final int record_count = 3;
		
		List<String> data = getRandomDataAsList(record_count);
		
		for (String dat : data) {
			bean.setSetStringProp.add(dat);
		}
		
		session.save(bean);
		
		assertNotNull(bean.getId());
		assertNotNull(bean.id);
		
		String idValue = bean.id;
		
		logger.info("### QUERYING ID: " + idValue + " ... ");
		
		MyBean bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		
		assertEquals(idValue, bean2.id);
		assertEquals(idValue, bean2.getId());
		
		assertNotNull(bean2.setSetStringProp);
		
		Iterator<String> it = bean2.setSetStringProp.iterator();
		
		assertNotNull(it);
		
		List<String> data2 = new ArrayList<String>(data);
		
		logger.info("### LISTING ELEMENTS FOR ID: " + idValue + " ... ");
		
		int c = 0;
		
		for (; it.hasNext(); ) {
			String dat = it.next();
			
			logger.debug("### dat: " + dat + " " + c++);
			
			assertNotNull(dat);
			
			assertTrue(data2.contains(dat));
			
			data2.remove(dat);
		}
		
		assertEquals(0, data2.size());
		
		assertEquals(record_count, c);
		
		for (String key : data) {
			bean2.setSetStringProp.remove(key);
		}
		
		bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		
		assertEquals(idValue, bean2.id);
		assertEquals(idValue, bean2.getId());
		
		assertNotNull(bean2.setSetStringProp);
		
		it = bean2.setSetStringProp.iterator();
		
		c = 0;
		
		for (; it.hasNext(); ) {
			c++;
		}
		
		assertEquals(0, c);
		
		for (String key : data) {
			bean2.setSetStringProp.add(key);
		}
		
		data2 = new ArrayList<String>(data);
		
		logger.info("### LISTING ELEMENTS FOR ID: " + idValue + " ... ");
		
		it = bean2.setSetStringProp.iterator();
		
		c = 0;
		
		for (; it.hasNext(); ) {
			String dat = it.next();
			
			logger.debug("### dat: " + dat + " " + c++);
			
			assertNotNull(dat);
			
			assertTrue(data2.contains(dat));
			
			data2.remove(dat);
		}
		
		assertEquals(0, data2.size());
		
		assertEquals(record_count, c);
	}

	@Test
	public void testSetProprety_WithPaging() throws Exception {
		path_bean_xml = "MyBean_with_native_set_prop.hbm.xml";
		
		configure(true);
		
		MyBean bean = new MyBean();
		
		bean.setSetStringProp = new HashSet<String>();
		
		final int record_count = 220;
		
		List<String> data = getRandomDataAsList(record_count);
		
		for (String dat : data) {
			bean.setSetStringProp.add(dat);
		}
		
		session.save(bean);
		
		assertNotNull(bean.getId());
		assertNotNull(bean.id);
		
		String idValue = bean.id;
		
		logger.info("### QUERYING ID: " + idValue + " ... ");
		
		MyBean bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		
		assertEquals(idValue, bean2.id);
		assertEquals(idValue, bean2.getId());
		
		assertNotNull(bean2.setSetStringProp);
		
		Iterator<String> it = bean2.setSetStringProp.iterator();
		
		assertNotNull(it);
		
		List<String> data2 = new ArrayList<String>(data);
		
		logger.info("### LISTING ELEMENTS FOR ID: " + idValue + " ... ");
		
		int c = 0;
		
		for (; it.hasNext(); ) {
			String dat = it.next();
			
			logger.debug("### dat: " + dat + " " + c++);
			
			assertNotNull(dat);
			
			assertTrue(data2.contains(dat));
			
			data2.remove(dat);
		}
		
		assertEquals(0, data2.size());
		
		assertEquals(record_count, c);
		
		for (String key : data) {
			bean2.setSetStringProp.remove(key);
		}
		
		bean2 = session.find(MyBean.class, idValue);
		
		assertNotSame(bean, bean2);
		
		assertNotNull(bean2);
		
		assertEquals(idValue, bean2.id);
		assertEquals(idValue, bean2.getId());
		
		assertNotNull(bean2.setSetStringProp);
		
		it = bean2.setSetStringProp.iterator();
		
		c = 0;
		
		for (; it.hasNext(); ) {
			c++;
		}
		
		assertEquals(0, c);
		
		for (String key : data) {
			bean2.setSetStringProp.add(key);
		}
		
		data2 = new ArrayList<String>(data);
		
		logger.info("### LISTING ELEMENTS FOR ID: " + idValue + " ... ");
		
		it = bean2.setSetStringProp.iterator();
		
		c = 0;
		
		for (; it.hasNext(); ) {
			String dat = it.next();
			
			logger.debug("### dat: " + dat + " " + c++);
			
			assertNotNull(dat);
			
			assertTrue(data2.contains(dat));
			
			data2.remove(dat);
		}
		
		assertEquals(0, data2.size());
		
		assertEquals(record_count, c);
	}
	
	private List<String> getRandomDataAsList(int record_count) {
		List<String> dat = new ArrayList<String>();
		
		for (int i = 0; i < record_count; i++) {
			String _ = "dat #" + i + " " + TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			
			dat.add(_);
		}
		
		return dat;
	}
}
