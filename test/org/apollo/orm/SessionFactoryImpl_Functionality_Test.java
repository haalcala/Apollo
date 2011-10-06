package org.apollo.orm;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

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
	public void testProperties() throws Exception {
		{
			path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_int_prop.hbm.xml";
			
			List<Integer> values = new ArrayList<Integer>();

			values.add(0);
			values.add(Integer.MIN_VALUE);
			values.add(Integer.MAX_VALUE);

			testProp("intProp", Integer.TYPE, values);
		}
		
		{
			path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_long_prop.hbm.xml";
			
			List<Long> values = new ArrayList<Long>();
			
			values.add(0L);
			values.add(Long.MIN_VALUE);
			values.add(Long.MAX_VALUE);
			
			testProp("longProp", Long.TYPE, values);
		}
		
		{
			path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_double_prop.hbm.xml";
			
			List<Double> values = new ArrayList<Double>();
			
			values.add(0D);
			values.add(Double.MIN_VALUE);
			values.add(Double.MAX_VALUE);
			
			testProp("doubleProp", Double.TYPE, values);
		}
		
		{
			path_bean_xml = "org/apollo/orm/beans/MyBean_with_one_float_prop.hbm.xml";
			
			List<Float> values = new ArrayList<Float>();
			
			values.add(0F);
			values.add(Float.MIN_VALUE);
			values.add(Float.MAX_VALUE);
			
			testProp("floatProp", Float.TYPE, values);
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
	
	void testProp(String prop, Class<?> clazz, List<?> values) throws Exception {
		MyBean bean;
		
		for (Object value : values) {
			logger.debug("###########################################################################################");
			logger.debug("#### Testing Data Storage for " + clazz + " property: " + prop + " value: " + value);
			
			configure(true);
			
			bean = new MyBean();

			String getMethodName = SessionImpl.getGetMethodFromProperty(prop);
			
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
}
