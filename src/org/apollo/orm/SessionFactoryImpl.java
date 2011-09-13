package org.apollo.orm;

import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import net.sf.ehcache.CacheManager;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.log4j.Logger;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class SessionFactoryImpl implements SessionFactory {
	private Logger logger = Logger.getLogger(SessionFactoryImpl.class);
	
	Map<Class<?>, ClassConfig> classToClassConfig;
	
	Map<String, ClassConfig> columnToClassConfig;

	Properties conf;
	
	CassandraKeyspaceWrapper keyspaceWrapper;

	Map<String, CassandraColumnFamilyWrapper> cfCache = new HashMap<String, CassandraColumnFamilyWrapper>();
	
	CacheManager cacheManager = new CacheManager(ClassLoader.getSystemResourceAsStream("cassanate-ehcache.xml"));
	
	SAXBuilder builder = new SAXBuilder();
	
	public SessionFactoryImpl(Properties conf) throws Exception {
		this.conf = conf;
	}


	public void addClassConfiguration(String pathToXml) throws ApolloException {
		addClassConfiguration(ClassLoader.getSystemResourceAsStream(pathToXml));
	}
	
	public void addClassConfiguration(InputStream is) throws ApolloException {
		long ctm = System.currentTimeMillis();
		try {
			long ctm2 = System.currentTimeMillis();
			builder.setValidation(false);
			Document document = builder.build(is);
			if (logger.isDebugEnabled()) logger.debug("Document document = new SAXBuilder().build(is) ["+(System.currentTimeMillis()-ctm2)+"ms]");
			
			List<?> contents = document.getContent();
			
			for (Object object : contents) {
				if (logger.isDebugEnabled()) logger.debug(object.getClass());
				
				if (object instanceof Element) {
					Element element = (Element) object;
					
					if (logger.isDebugEnabled()) logger.debug("*** " + element.getName() + " ***");
					
					if (element.getName().equals("hibernate-mapping")) {
						List<?> classes = element.getContent();
						
						for (Object object2 : classes) {
							if (object2 instanceof Element) {
								Element element2 = (Element) object2;
								
								if (element2.getName().equals("class")) {
									List<?> attributes2 = element2.getAttributes();
									
									ClassConfig classConfig = new ClassConfig();
									
									for (Object object3 : attributes2) {
										if (logger.isDebugEnabled()) logger.debug(object3);
										
										if (object3 instanceof Attribute) {
											Attribute attribute = (Attribute) object3;
											
											String attr = attribute.getName();
											String val = attribute.getValue();
											
											if (attr.equals("name")) {
												classConfig.setClazz(Class.forName(val));
											}
											else if (attr.equals("table")) {
												classConfig.setColumnFamily(val);
												
											}
										}
									}
									
									List properties = element2.getContent();
									
									for (Object _property : properties) {
										if (_property instanceof Element) {
											Element property = (Element) _property;
											
											String prop = property.getName();
											
											if (logger.isDebugEnabled()) logger.debug("*** " + prop);
											
											if (property.getName().equals("id")) {
												String method_name = getAttribute(property, "name", null);
												
												classConfig.idMethod = method_name;
												classConfig.idColumn = method_name;
												classConfig.idUnsaved = getAttribute(property, "unsaved-value", null);
												
												classConfig.idUnsaved = classConfig.idUnsaved != null && classConfig.idUnsaved.equals("null") ? null : classConfig.idUnsaved;
												
												List idElements = property.getContent();
												
												for (Object object3 : idElements) {
													if (object3 instanceof Element) {
														Element element3 = (Element) object3;

														if (element3.getName().equals("column")) {
															classConfig.idColumn = getAttribute(element3, "name", classConfig.idColumn);
														}
														else if (element3.getName().equals("generator")) {
															String _gen = getAttribute(element3, "class", "native");
															
															if (_gen != null && _gen.equals("native"))
																classConfig.idGenerator = TimeUUIDType.class;
														}
													}
												}
											}
											else if (property.getName().equals("property") 
													|| property.getName().equals("many-to-one")
													|| property.getName().equals("set")) {
												List attributes3 = property.getAttributes();
												
												String method_name = property.getAttribute("name").getValue();
												
												if (logger.isDebugEnabled()) logger.debug("method_name: " + method_name);
												
												boolean hasColumn = false;
												boolean needsColumn = true;
												
												for (Object object4 : attributes3) {
													if (object4 instanceof Attribute) {
														Attribute attribute = (Attribute) object4;
														
														String attr = attribute.getName();
														String val = attribute.getValue();
														
														if (!attr.equals("name")) {
															if (logger.isDebugEnabled()) logger.debug("\t" + attr + " : " + val);
															
															classConfig.setMethodConfig(method_name, attr, val);
															
															if (attr.equals("column"))
																hasColumn = true;
														}
													}
												}
												
												if (property.getName().equals("set")) {
													needsColumn = false;
													
													Element elmManyToMany = property.getChild("many-to-many");
													
													Attribute attClass = elmManyToMany.getAttribute("class");
													
													String attClass_val = attClass.getValue();
													
													if (logger.isDebugEnabled()) logger.debug("attClass_val: " + attClass_val);
													
													classConfig.setMethodConfig(method_name, "class", attClass_val);
												}
												
												if (!hasColumn && needsColumn)
													classConfig.setMethodConfig(method_name, "column", method_name);
											}
											else {
												
											}
										}
									}
									
									if (classToClassConfig == null)
										classToClassConfig = new HashMap<Class<?>, ClassConfig>();
									
									if (!classToClassConfig.containsKey(classConfig.clazz))
										classToClassConfig.put(classConfig.clazz, classConfig);
									
									if (columnToClassConfig == null)
										columnToClassConfig = new HashMap<String, ClassConfig>();
									
									if (!columnToClassConfig.containsKey(classConfig.cfName))
										columnToClassConfig.put(classConfig.cfName, classConfig);
									
									if (logger.isDebugEnabled()) logger.debug("ClassConfig: " + classConfig);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ApolloException(e);
		}
		finally {
			if (logger.isDebugEnabled()) logger.debug("addClassConfiguration - done ["+(System.currentTimeMillis()-ctm)+"ms]");
		}
	}

	private String getAttribute(Element property, String attribute, String _default) {
		Attribute attr = property.getAttribute(attribute);
		
		return attr == null ? _default : attr.getValue();
	}

	public void validate() throws ApolloException {
		try {
			keyspaceWrapper = new CassandraKeyspaceWrapper(conf);
			
			if (classToClassConfig == null)
				return;
			
			for (ClassConfig cc : classToClassConfig.values()) {
				CassandraColumnFamilyWrapper cf = getCassandraColumnFamilyWrapper(cc.cfName);
				
				if (!cf.columnFamilyExists())
					cf.createColumnFamily();
				
				if (logger.isDebugEnabled())
					logger.debug(cc.cfName + " : cf.isColumnIndexed(\"__rstat__\"): " + cf.isColumnIndexed("__rstat__"));
				
				if (!cf.isColumnIndexed("__rstat__"))
					cf.updateColumnFamilyMetaData("__rstat__", ColumnIndexType.KEYS, ComparatorType.UTF8TYPE);

				Set<String> methods = cc.getMethods();
				
				if (methods != null) {
					for (String prop : methods) {
						String table = cc.getMethodConfig(prop).get("table");
						
						if (table != null) {
							cf = getCassandraColumnFamilyWrapper(table);

							if (!cf.columnFamilyExists())
								cf.createColumnFamily();
						}
					}
				}
			}
			
			for (ClassConfig cc : classToClassConfig.values()) {
				Method[] methods = cc.clazz.getMethods();
				
				Set<String> props = cc.getMethods();

				if (props != null) {
					for (String prop : cc.getMethods()) {
						Map<String, String> method_config = cc.getMethodConfig(prop);

						Class<?> get_method_type = null, set_method_type = null;

						String set_method_name = "set" + prop.substring(0, 1).toUpperCase() + prop.substring(1);
						String get_method_name = "get" + prop.substring(0, 1).toUpperCase() + prop.substring(1);
						String is_method_name = "is" + prop.substring(0, 1).toUpperCase() + prop.substring(1);
						
						Method get_method = null;

						for (int i = 0; i < methods.length; i++) {
							Method method = methods[i];

							if (method.getName().equals(get_method_name) || method.getName().equals(is_method_name)) {
								get_method_type = method.getReturnType();
								
								get_method = method;
							}
							else if (method.getName().equals(set_method_name)) {
								Class<?>[] paramTypes = method.getParameterTypes();

								if (paramTypes.length == 1) {
									set_method_type = paramTypes[0];
								}
							}
						}

						if (get_method_type == null || set_method_type == null)
							throw new IllegalAccessException("there must be a pair of 'get' and 'set' access methods for property '" + prop + "' of class '" + cc.clazz + "'");

						if (get_method_type != set_method_type)
							throw new IllegalAccessException("The 'get' and 'set' access methods for property '" + prop + "' of class '" + cc.clazz + "' must be of the same type");

						if (method_config.get("type") == null) {
							String method_type = get_method_type.getCanonicalName();

							if (get_method_type.isPrimitive())
								method_type = get_method_type.getCanonicalName();

							if (get_method_type == Integer.TYPE)
								method_type = Integer.class.getCanonicalName();
							else if (get_method_type == Long.TYPE)
								method_type = Long.class.getCanonicalName();
							else if (get_method_type == Byte.TYPE)
								method_type = Byte.class.getCanonicalName();
							else if (get_method_type == Double.TYPE)
								method_type = Double.class.getCanonicalName();
							else if (get_method_type == Float.TYPE)
								method_type = Float.class.getCanonicalName();
							else if (get_method_type == Short.TYPE)
								method_type = Short.class.getCanonicalName();
							else if (get_method_type == Boolean.TYPE)
								method_type = Boolean.class.getCanonicalName();
							else if (get_method_type == Map.class) {
								Type return_type = get_method.getGenericReturnType();
								
								if (logger.isDebugEnabled())
									logger.debug(get_method.getName() + " return_type: " + return_type);
								
								if (return_type instanceof ParameterizedType) {
									Type[] types = ((ParameterizedType) return_type).getActualTypeArguments();
									
									if (types != null && types[0] == String.class && types[1].toString().startsWith(Map.class.getCanonicalName())) {
										if (logger.isDebugEnabled())
											logger.debug("Prop '" + prop + "' is a mapOfMaps!!!");
										
										if (cc.mapOfMaps == null)
											cc.mapOfMaps = new ArrayList<String>();
										
										cc.mapOfMaps.add(prop);
									}
								}
							}

							method_config.put("type", method_type);
						}

						if (cc.propertyType == null)
							cc.propertyType = new HashMap<String, Class<?>>();

						cc.propertyType.put(prop, get_method_type);


						/*
						 * Determine if such a class should be proxied.
						 */
						String lazy_loaded = method_config.get("lazy-loaded");
						if (lazy_loaded != null && lazy_loaded.equalsIgnoreCase("true")) {
							cc.shouldProxy = true;

							if (cc.proxyMethods == null)
								cc.proxyMethods = new ArrayList<Method>();

							cc.proxyMethods.add(cc.getGetMethodFromCache(prop));
							cc.proxyMethods.add(cc.getSetMethodFromCache(prop));
						}

						if (cc.methodToProp == null)
							cc.methodToProp = new HashMap<String, String>();

						cc.methodToProp.put(SessionImpl.getGetMethodFromProperty(prop), prop);
						cc.methodToProp.put(SessionImpl.getSetMethodFromProperty(prop), prop);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			
			throw new ApolloException(e);
		}
	}

	public Session getSession() {
		Session session = new SessionImpl(this, keyspaceWrapper, classToClassConfig, columnToClassConfig);
		
		return session;
	}

	synchronized public CassandraColumnFamilyWrapper getCassandraColumnFamilyWrapper(String columnFamily) {
		CassandraColumnFamilyWrapper ret = cfCache.get(columnFamily);
		
		if (ret == null) {
			ret = new CassandraColumnFamilyWrapper(keyspaceWrapper, columnFamily);
			
			cfCache.put(columnFamily, ret);
		}
		
		return ret;
	}

	public CacheManager getCacheManager() {
		return cacheManager;
	}

	public void shutdown() {
		if (keyspaceWrapper != null) {
			ThriftCluster cluster = keyspaceWrapper.getCluster();
			
			cluster.getConnectionManager().shutdown();
		}
	}

	public CassandraKeyspaceWrapper getCassandraKeyspaceWrapper() {
		return keyspaceWrapper;
	}
}
