package org.apollo.orm;

import java.util.Properties;

public class Configurator {
	private SessionFactory sessionFactory;
	
	public Configurator(Properties conf) throws Exception {
		sessionFactory = new SessionFactoryImpl(conf);
	}
	
	public void addClassConfiguration(String pathToXml) throws CassanateException {
		sessionFactory.addClassConfiguration(pathToXml);
	}
	
	public SessionFactory configure() throws CassanateException {
		sessionFactory.validate();
		
		return sessionFactory;
	}
}
