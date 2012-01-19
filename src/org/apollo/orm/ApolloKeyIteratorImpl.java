package org.apollo.orm;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApolloKeyIteratorImpl implements ApolloIterator<String> {
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private String rowKey;
	
	private String startCol;
	
	private String endCol;
	
	private int maxCols;
	
	private CassandraColumnFamilyWrapper cf;
	
	private Map<String, Serializable> cols;
	
	private long keyCount;
	
	private int itemsPerPage;
	
	private boolean scanRows;
	
	private int maxItems;
	
	public ApolloKeyIteratorImpl(CassandraColumnFamilyWrapper cf, String rowKey, int maxRows, String startCol, String endCol, int maxCols, int itemsPerPage, boolean scanRows) {
		this.cf = cf;
		this.rowKey = rowKey;
		this.startCol = startCol;
		this.endCol = endCol;
		this.maxCols = maxCols;
		this.itemsPerPage = itemsPerPage;
		this.scanRows = scanRows;
		
		if (scanRows) {
			maxItems = maxRows;
			curKey = rowKey != null ? rowKey : "";
		}
		else {
			maxItems = maxCols;
		}
	}
	
	public String curKey;
	
	public Iterator<String> it;
	
	public int keyi = -1;

	private String prop;
	
	public void remove() {
		if (curKey == null)
			throw new NoSuchElementException();
		
		cf.deleteColumn(rowKey, curKey);
	}
	
	public String next() {
		if (it != null) {
			curKey = it.next();
			
			keyi++;
			
			keyCount++;
			
			return curKey;
		}
		
		throw new NoSuchElementException();
	}
	
	private void loadNewPage() throws Exception {
		if (keyi == -1 || keyi > itemsPerPage) {
			if (logger.isDebugEnabled())
				logger.debug("keyi: " + keyi);
			
			if (scanRows) {
				Map<String, Map<String, Serializable>> rows = cf.getColumnsAsMap(curKey, "", "", "", 1, itemsPerPage + 1 + (keyi > itemsPerPage ? 1 : 0), true);
				
				if (rows != null && rows.size() > 0) {
					it = rows.keySet().iterator();
					
					if (keyi > itemsPerPage) // discard the first result
						it.next();
					
					keyi = 0;
				}
			}
			else {
				Map<String, Map<String, Serializable>> rows = cf.getColumnsAsMap(rowKey, "", curKey == null ? "" : curKey, "", 
						itemsPerPage + 1 + (keyi > itemsPerPage ? 1 : 0), 1, scanRows);
				
				if (rows != null && rows.size() > 0) {
					cols = rows.get(rowKey);

					if (cols != null) {
						it = cols.keySet().iterator();

						if (keyi > itemsPerPage) // discard the first result
							it.next();

						keyi = 0;
					}
				}
			}
		}
	}
	
	public boolean hasNext() {
		if (maxItems > 0 && keyCount >= maxItems) {
			it = null;

			return false;
		}
		
		try {
			loadNewPage();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		if (it != null)
			return it.hasNext();
		
		return false;
	}
	
}
