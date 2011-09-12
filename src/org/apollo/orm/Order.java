package org.apollo.orm;

public class Order {
	public static final String ASC = "asc";
	public static final String DESC = "asc";
	
	private String column;
	private String order;
	
	private Order(String column, String order) {
		this.column = column;
		this.order = order;
	}
	
	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getOrder() {
		return order;
	}

	public void setOrder(String order) {
		this.order = order;
	}

	public static Order asc(String column) {
		return new Order(column, ASC);
	}
	
	public static Order desc(String column) {
		return new Order(column, DESC);
	}
}
