package org.apollo.orm;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class MyInterceptor implements MethodInterceptor {
	private Logger logger = Logger.getLogger(MyInterceptor.class);

	private Object realObj;
	private ClassConfig classConfig;
	private SessionFactory factory;
	private String idValue;
	private CassandraColumnFamilyWrapper cf;

	public MyInterceptor(String idValue, Object obj, SessionFactory factory, ClassConfig classConfig) {
		this.realObj = obj;
		this.factory = factory;
		this.classConfig = classConfig;
		this.idValue = idValue;

		this.cf = factory.getCassandraColumnFamilyWrapper(classConfig.cfName);
	}

	@SuppressWarnings("unchecked")
	public Object intercept(Object o,
			Method method,
			Object[] objects,
			MethodProxy methodProxy) throws Throwable {
		Object res = null;

		/*
		if (classConfig.proxyMethods != null && classConfig.proxyMethods.contains(method)) {
			String prop = classConfig.methodToProp.get(method.getName());

			if (logger.isDebugEnabled())
				logger.debug("Before '" + method.getName() + "' prop: " + prop);
			
			long time1 = System.currentTimeMillis();

			boolean ensure_lazy_loaded_prop_entry = false;

			boolean mapOfMaps = classConfig.isMapOfMaps(prop);

			SessionImpl session = (SessionImpl) factory.getSession();

			String idValue = (String) classConfig.getIdValue(realObj);

			Map<String, String> method_config = classConfig.getMethodConfig(prop);

			String table = method_config.get(SessionImpl.ATTR_TABLE);
			String child_table_key_suffix = method_config.get(SessionImpl.ATTR_CHILD_TABLE_KEY_SUFFIX);

			CassandraColumnFamilyWrapper child_table_cf = session.getSessionFactory().getCassandraColumnFamilyWrapper(table);

			String columnIdValue = idValue + (child_table_key_suffix != null ? child_table_key_suffix : "");

			if (method.getName().startsWith("set")) {
				Map<String, ?> map = null;

				if (objects[0] != null) {
					if (!Enhancer.isEnhanced(objects[0].getClass()))
						map = (Map<String, ?>) objects[0];
					
					CassandraColumnFamilyWrapper hashmap_cf = child_table_cf != null ? child_table_cf : cf;
					
					if (mapOfMaps)
						map = new ApolloMapImpl<Map<String, String>>(factory, null, idValue, hashmap_cf, prop, (Map<String, Map<String, String>>) map, mapOfMaps, null);
					else
						map = new ApolloMapImpl<String>(factory, null, idValue, hashmap_cf, prop, (Map<String, String>) map, mapOfMaps, null);
				}

				objects[0] = map;

				ensure_lazy_loaded_prop_entry = true;

				res = method.invoke(realObj, objects);
			}
			else if (method.getName().startsWith("get")) {
				if (logger.isDebugEnabled())
					logger.debug("Processing 'get' request ...");

				res = classConfig.getPropertyMethodValue(realObj, prop);

				if (logger.isDebugEnabled())
					logger.debug("res : " + res);

				if (res == null) {
					Map<String, ?> map = null;
					
					if (mapOfMaps) {
						map = new ApolloMapImpl<Map<String, String>>(factory, null, columnIdValue, child_table_cf, prop, null, mapOfMaps, null);
					}
					else {
						map = new ApolloMapImpl<String>(factory, null, columnIdValue, child_table_cf, prop, null, mapOfMaps, null);
					}

					classConfig.setPropertyMethodValue(realObj, prop, map);
					
					res = map;

					ensure_lazy_loaded_prop_entry = true;
				}
			}

			if (ensure_lazy_loaded_prop_entry) {
				if (((SessionImpl) session).lazyLoadedProps == null)
					((SessionImpl) session).lazyLoadedProps = new HashMap<String, List<String>>();

				String cache_key = ((SessionImpl) session).getCacheKey(classConfig.clazz, idValue);

				List<String> list = ((SessionImpl) session).lazyLoadedProps.get(cache_key);

				if (list == null) {
					list = new ArrayList<String>();
					((SessionImpl) session).lazyLoadedProps.put(cache_key, list);
				}

				if (!list.contains(prop)) {
					list.add(prop);

					if (logger.isDebugEnabled())
						logger.debug(cache_key + " added to the lazy loaded prop " + classConfig.clazz + "|" + prop);
				}
			}

			if (logger.isDebugEnabled())
				logger.debug("After'" + method.getName() + "' prop: " + prop);
			if (logger.isDebugEnabled())
				logger.debug("Took: " + (System.currentTimeMillis() - time1) + " ms");
		}
		else {
			res = method.invoke(realObj, objects);
		}*/

		return res;
	}   
} 