package org.apollo.orm;

public interface GetColumnsHandler {
	boolean scanAllRows();
	
	boolean scanAllColumns();
	
	void onStartRow(String rowKey);
	
	void onEndRow(String rowKey);
	
	void onColumn(String key, String val);

	int getColumnsPerRequest();

	int getRowsPerRequest();

	boolean skipRow();

	boolean scanRows();

	boolean scanCols();

	boolean skipCol();
}
