package org.apollo.orm;

import java.util.Properties;

public interface TestConstants {
	static class Util {
		public static Properties getTestConf() throws Exception {
			Properties ret = new Properties();
			
			ret.load(ClassLoader.getSystemResourceAsStream("apollo.conf"));
			
			return ret;
		}
	}
}
