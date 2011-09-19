package org.apollo.orm;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;

import org.apollo.orm.beans.Simple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreTest {

	private static SessionFactory factory;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Properties prop = new Properties();
		
		prop.load(ClassLoader.getSystemResourceAsStream("cassandra.conf"));
		
		Configurator configurator = new Configurator(prop);
		
		configurator.addClassConfiguration("org/apollo/orm/beans/Simple.hbm.xml");
		
		factory = configurator.configure();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		factory.shutdown();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testSimple() throws Exception {
		Session ss = factory.getSession();
		
		ss.truncate(Simple.class);
		
		String myId = "My Id";
		
		Simple simple = new Simple();
		simple.setId(myId);
		
		ss.save(simple);
	}

}
