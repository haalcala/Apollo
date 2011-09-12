package org.apollo.orm;

/**
 * @author harold
 *
 */
public interface GetSuperColumnsHandler {
	void onStartRow(String rowKey);
	
	void onStartSuperColumn(String superColumnName);
	
	void onColumn(String columnName, String columnValue);
	
	void onEndSuperColumn(String superColumnName);
	
	void onEndRow(String rowKey);
	
	int getRowsPerPage();
	
	/**
	 * is evaluated before scanning columns
	 */
	boolean scanColumns();
	
	/**
	 * is evaluated after a row is detected whether to break the loop or continue scanning rows
	 */
	boolean scanRows();

	
	/**
	 * is evaluated after a row is detected whether to issue 'continue' in the loop
	 * 
	 * @return
	 */
	boolean skipRow();
}
