package org.apollo.orm;

import java.util.Properties;

import org.apache.log4j.Logger;

public class Configurator {
	Logger logger = Logger.getLogger(getClass());
	
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
