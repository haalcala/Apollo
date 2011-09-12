package org.apollo.orm;

public class GetSuperColumnsHandlerAdapter implements GetSuperColumnsHandler {

	public void onStartRow(String rowKey) {
	}

	public void onStartSuperColumn(String superColumnName) {
	}

	public void onColumn(String columnName, String columnValue) {
	}

	public void onEndSuperColumn(String superColumnName) {
	}

	public void onEndRow(String rowKey) {
	}

	public int getRowsPerPage() {
		return 100;
	}

	public boolean scanColumns() {
		return true;
	}

	public boolean scanRows() {
		return true;
	}

	public boolean skipRow() {
		return false;
	}
}
