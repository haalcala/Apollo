package org.apollo.orm;

import java.util.List;

public class Expression {
	public static final String OPERATION_EQ = "EQ";
	public static final String OPERATION_GT = "GT";
	public static final String OPERATION_GTE = "GTE";
	public static final String OPERATION_LT = "LT";
	public static final String OPERATION_LTE = "LTE";
	public static final String OPERATION_NOT = "NOT";
	public static final String OPERATION_IN = "IN";
	
	private Object expected;
	private String operation;
	private String prop;
	
	private Expression(String operation, String property, Object expected) {
		this.operation = operation;
		this.expected = expected;
		this.prop = property;
	}

	public static Expression eq(String property, Object expected) {
		return new Expression(OPERATION_EQ, property, expected);
	}

	public static Expression gt(String property, Object expected) {
		return new Expression(OPERATION_GT, property, expected);
	}
	
	public static Expression gte(String property, Object expected) {
		return new Expression(OPERATION_GTE, property, expected);
	}
	
	public static Expression lt(String property, Object expected) {
		return new Expression(OPERATION_LT, property, expected);
	}
	
	public static Expression lte(String property, Object expected) {
		return new Expression(OPERATION_LTE, property, expected);
	}

	public static Expression in(String property, List<String> keys) {
		return new Expression(OPERATION_IN, property, keys);
	}
	
	public Object getExpected() {
		return expected;
	}

	public String getOperation() {
		return operation;
	}

	public String getProperty() {
		return prop;
	}
}
