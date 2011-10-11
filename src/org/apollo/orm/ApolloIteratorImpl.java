package org.apollo.orm;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

public class ApolloIteratorImpl implements ApolloIterator<String> {
	Logger logger = Logger.getLogger(getClass());
	
	private String rowKey;
	
	private String startCol;
	
	private String endCol;
	
	private int maxCols;
	
	private CassandraColumnFamilyWrapper cf;
	
	private ClassConfig classConfig;
	
	private Map<String, String> cols;
	
	private long colCount;
	
	public ApolloIteratorImpl(CassandraColumnFamilyWrapper cf, ClassConfig classConfig, String rowKey, String startCol, String endCol) {
		this(cf, classConfig, rowKey, startCol, endCol, 0);
	}
	
	public ApolloIteratorImpl(CassandraColumnFamilyWrapper cf, ClassConfig classConfig, String rowKey, String startCol, String endCol, int maxCols) {
		this.classConfig = classConfig;
		this.cf = cf;
		this.rowKey = rowKey;
		this.startCol = startCol;
		this.endCol = endCol;
		this.maxCols = maxCols;
	}
	
	public String curCol;
	
	public Iterator<String> col_it;
	
	public int coli = -1;
	
	@Override
	public void remove() {
		if (curCol == null)
			throw new NoSuchElementException();
		
		cf.deleteColumn(rowKey, curCol);
	}
	
	@Override
	public String next() {
		if (col_it != null) {
			curCol = col_it.next();
			
			coli++;
			
			colCount++;
			
			return curCol;
		}
		
		throw new NoSuchElementException();
	}
	
	void loadNewPage() {
		if (coli == -1 || coli > ApolloConstants.MAX_COLUMN_PER_PAGE) {
			if (logger.isDebugEnabled())
				logger.debug("coli: " + coli);
			
			Map<String, Map<String, String>> rows = cf.getColumnsAsMap(rowKey, "", curCol == null ? "" : curCol, "", 
					ApolloConstants.MAX_COLUMN_PER_PAGE + 1 + (coli > ApolloConstants.MAX_COLUMN_PER_PAGE ? 1 : 0), 1);
			
			if (rows != null && rows.size() > 0) {
				cols = rows.get(rowKey);
				
				col_it = cols.keySet().iterator();
				
				if (coli > ApolloConstants.MAX_COLUMN_PER_PAGE) // discard the first result
					col_it.next();
				
				coli = 0;
			}
		}
	}
	
	@Override
	public boolean hasNext() {
		if (maxCols > 0 && colCount >= maxCols) {
			col_it = null;
			
			return false;
		}
		
		loadNewPage();
		
		if (col_it != null)
			return col_it.hasNext();
		
		return false;
	}
}
