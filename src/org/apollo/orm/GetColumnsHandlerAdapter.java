package org.apollo.orm;

public class GetColumnsHandlerAdapter implements
		GetColumnsHandler {

	public boolean scanAllRows() {
		return true;
	}

	public boolean scanAllColumns() {
		return true;
	}

	public void onStartRow(String rowKey) {
	}

	public void onEndRow(String rowKey) {
	}

	public void onColumn(String key, String val) {
	}

	public int getColumnsPerRequest() {
		return 100;
	}

	public int getRowsPerRequest() {
		return 100;
	}

	public boolean skipRow() {
		return false;
	}

	public boolean scanRows() {
		return true;
	}

	public boolean scanCols() {
		return true;
	}

	public boolean skipCol() {
		return false;
	}

}
