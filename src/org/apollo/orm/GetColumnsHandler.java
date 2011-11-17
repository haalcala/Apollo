package org.apollo.orm;

import java.io.Serializable;

public interface GetColumnsHandler {
	boolean scanAllRows();
	
	boolean scanAllColumns();
	
	void onStartRow(String rowKey);
	
	void onEndRow(String rowKey);
	
	void onColumn(String key, Serializable val);

	int getColumnsPerRequest();

	int getRowsPerRequest();

	boolean skipRow();

	boolean scanRows();

	boolean scanCols();

	boolean skipCol();
}
