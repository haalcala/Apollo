package org.apollo.orm;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configurator {
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private SessionFactory sessionFactory;
	
	public Configurator(Properties conf) throws Exception {
		sessionFactory = new SessionFactoryImpl(conf);
	}
	
	public void addClassConfiguration(String pathToXml) throws ApolloException {
		sessionFactory.addClassConfiguration(pathToXml);
	}
	
	public SessionFactory configure() throws ApolloException {
		if (logger.isDebugEnabled())
			logger.debug("Trying to configure ..");
		
		sessionFactory.validate();
		
		return sessionFactory;
	}
}
