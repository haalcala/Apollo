package org.apollo.orm;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import net.sf.ehcache.CacheManager;

import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionFactoryImpl implements SessionFactory, ApolloConstants {
	public static final String ATTR_TYPE = "type";

	private Logger logger = LoggerFactory.getLogger(SessionFactoryImpl.class);
	
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
			
			if (logger.isDebugEnabled()) 
				logger.debug("Document document = new SAXBuilder().build(is) ["+(System.currentTimeMillis()-ctm2)+"ms]");
			
			List<?> contents = document.getContent();
			
			for (Object object : contents) {
				if (logger.isDebugEnabled()) logger.debug("{}", object.getClass());
				
				if (object instanceof Element) {
					Element element = (Element) object;
					
					if (logger.isDebugEnabled()) 
						logger.debug("*** " + element.getName() + " ***");
					
					if (element.getName().equals(NODE_HIBERNATE_MAPPING)) {
						List<?> classes = element.getContent();
						
						for (Object object2 : classes) {
							if (object2 instanceof Element) {
								Element element2 = (Element) object2;
								
								if (element2.getName().equals(ATTR_CLASS)) {
									List<?> attributes2 = element2.getAttributes();
									
									ClassConfig classConfig = new ClassConfig();
									
									for (Object object3 : attributes2) {
										if (logger.isDebugEnabled()) 
											logger.debug("{}", object3);
										
										if (object3 instanceof Attribute) {
											Attribute attribute = (Attribute) object3;
											
											String attr = attribute.getName();
											String val = attribute.getValue();
											
											if (attr.equals(ATTR_NAME)) {
												classConfig.setClazz(Class.forName(val));
											}
											else if (attr.equals(ATTR_TABLE)) {
												classConfig.setColumnFamily(val);
												
											}
										}
									}
									
									List properties = element2.getContent();
									
									for (Object _property : properties) {
										if (_property instanceof Element) {
											Element property = (Element) _property;
											
											String prop = property.getName();
											
											if (logger.isDebugEnabled()) 
												logger.debug("*** " + prop);
											
											if (property.getName().equals(NODE_ID)) {
												String method_name = getAttribute(property, ATTR_NAME, null);
												
												classConfig.idMethod = method_name;
												classConfig.idColumn = method_name;
												classConfig.idUnsaved = getAttribute(property, ATTR_UNSAVED_VALUE, null);
												
												classConfig.idUnsaved = classConfig.idUnsaved != null && classConfig.idUnsaved.equals(STR_NULL) ? null : classConfig.idUnsaved;
												
												List idElements = property.getContent();
												
												for (Object object3 : idElements) {
													if (object3 instanceof Element) {
														Element element3 = (Element) object3;

														if (element3.getName().equals(ATTR_COLUMN)) {
															classConfig.idColumn = getAttribute(element3, ATTR_NAME, classConfig.idColumn);
														}
														else if (element3.getName().equals(ATTR_GENERATOR)) {
															String _gen = getAttribute(element3, ATTR_CLASS, STR_NATIVE);
															
															if (_gen != null && _gen.equals(STR_NATIVE))
																classConfig.idGenerator = TimeUUIDType.class;
														}
													}
												}
											}
											else if (property.getName().equals(NODE_PROPERTY) 
													|| property.getName().equals(NODE_MANY_TO_ONE)
													|| property.getName().equals(NODE_ONE_TO_ONE)
													|| property.getName().equals(NODE_SET)) {
												List attributes3 = property.getAttributes();
												
												String method_name = property.getAttribute(ATTR_NAME).getValue();
												
												if (logger.isDebugEnabled()) 
													logger.debug("method_name: " + method_name);
												
												boolean hasColumn = false;
												boolean needsColumn = true;
												
												classConfig.setMethodConfig(method_name, "association", property.getName());
												
												for (Object object4 : attributes3) {
													if (object4 instanceof Attribute) {
														Attribute attribute = (Attribute) object4;
														
														String attr = attribute.getName();
														String val = attribute.getValue();
														
														if (!attr.equals(ATTR_NAME)) {
															if (logger.isDebugEnabled()) 
																logger.debug("\t" + attr + " : " + val);
															
															classConfig.setMethodConfig(method_name, attr, val);
															
															if (attr.equals("column"))
																hasColumn = true;
														}
													}
												}
												
												if (property.getName().equals(NODE_SET)) {
													needsColumn = false;
													
													Element elmManyToMany = property.getChild(NODE_MANY_TO_MANY);
													
													Attribute attClass = elmManyToMany.getAttribute(ATTR_CLASS);
													
													String attClass_val = attClass.getValue();
													
													if (logger.isDebugEnabled()) 
														logger.debug("attClass_val: " + attClass_val);
													
													classConfig.setMethodConfig(method_name, ATTR_CLASS, attClass_val);
												}
												
												if (!hasColumn && needsColumn)
													classConfig.setMethodConfig(method_name, ATTR_COLUMN, method_name);
											}
											else {
												logger.warn("Ignoring unrecognised tag '" + property.getName() + "'");
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
									
									if (logger.isDebugEnabled()) 
										logger.debug("ClassConfig: " + classConfig);
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
			if (logger.isDebugEnabled()) 
				logger.debug("addClassConfiguration - done ["+(System.currentTimeMillis()-ctm)+"ms]");
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
				Method[] methods = cc.clazz.getMethods();
				
				cc.setKeyspaceName(keyspaceWrapper.getKeyspaceName());

				Set<String> props = cc.getMethods();

				if (props != null) {
					for (String prop : cc.getMethods()) {
						Map<String, String> method_config = cc.getMethodConfig(prop);

						Class<?> get_method_type = null, set_method_type = null;

						String set_method_name = METHOD_PREFIX_SET + prop.substring(0, 1).toUpperCase() + prop.substring(1);
						String get_method_name = METHOD_PREFIX_GET + prop.substring(0, 1).toUpperCase() + prop.substring(1);
						String is_method_name = METHOD_PREFIX_IS + prop.substring(0, 1).toUpperCase() + prop.substring(1);
						
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

						if (method_config.get(ATTR_TYPE) == null) {
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

							method_config.put(ATTR_TYPE, method_type);
						}

						if (cc.propertyType == null)
							cc.propertyType = new HashMap<String, Class<?>>();
						
						if (logger.isDebugEnabled())
							logger.debug("The property '{}' is deteremined to be of type '{}'", prop, get_method_type);

						cc.propertyType.put(prop, get_method_type);

						if (cc.methodToProp == null)
							cc.methodToProp = new HashMap<String, String>();

						cc.methodToProp.put(SessionImpl.getGetMethodFromProperty(prop), prop);
						cc.methodToProp.put(SessionImpl.getSetMethodFromProperty(prop), prop);
					}
				}
			}

			/*
			 * Create/Update Schema
			 */
			logger.info("............................. Creating/Updating schema .............................");
			
			for (ClassConfig cc : classToClassConfig.values()) {
				CassandraColumnFamilyWrapper cf = getCassandraColumnFamilyWrapper(cc.cfName);

				if (!cf.columnFamilyExists())
					cf.createColumnFamily();

				if (logger.isDebugEnabled())
					logger.debug(cc.cfName + " : cf.isColumnIndexed(\"__rstat__\"): " + cf.isColumnIndexed(cc.getRStatColumnName()));

				if (!cf.isColumnIndexed(cc.getRStatColumnName()))
					cf.updateColumnFamilyMetaData(cc.getRStatColumnName(), ColumnIndexType.KEYS, IntegerType.instance);

				Set<String> methods = cc.getMethods();

				if (methods != null) {
					for (String prop : methods) {
						String table = cc.getMethodConfig(prop).get(ATTR_TABLE);

						if (table != null) {
							cf = getCassandraColumnFamilyWrapper(table);

							if (!cf.columnFamilyExists())
								cf.createColumnFamily();
						}
						
						Class<?> prop_type = cc.getPropertyType(prop);
						
						String column = cc.getMethodColumn(prop, false);
						
						Serializer<?> storage_serializer = cf.getColumnSerializer(column, false);
						
						logger.debug("Property: '" + prop + "' column: '" + column + "' propertyType: '" + prop_type + "' storageType: '" + storage_serializer + "'");

						if (Util.isNativelySupported(prop_type)) {
							if (storage_serializer == null) {
								cf.updateColumnFamilyMetaData(column, null, Util.getJavaTypeToCassandraType(prop_type));
							}
							else {
								Serializer<?> exp_serializer = Util.getJavaTypeSerialiser(prop_type);

								if (exp_serializer != storage_serializer) {
									if (prop_type == Integer.TYPE) {
									}
									else if (prop_type == Long.TYPE) {
									}
									else if (prop_type == Boolean.TYPE) {
									}
									else if (prop_type == Double.TYPE) {
									}
									else if (prop_type == Float.TYPE) {
									}
									else if (prop_type == Byte.TYPE) {
									}
									else if (prop_type == Character.class) {
									}
									else if (prop_type == String.class) {
									}
									else if (prop_type == Timestamp.class) {
									}
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
			Cluster cluster = keyspaceWrapper.getCluster();
			
			cluster.getConnectionManager().shutdown();
		}
	}

	public CassandraKeyspaceWrapper getCassandraKeyspaceWrapper() {
		return keyspaceWrapper;
	}
}
