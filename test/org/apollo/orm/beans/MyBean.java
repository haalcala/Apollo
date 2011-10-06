package org.apollo.orm.beans;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author harold
 *
 */
public class MyBean {
	public String id;
	
	public String stringProp;
	
	public Timestamp timestampProp;
	
	public int intProp;
	
	public long longProp;
	
	public double doubleProp;
	
	public byte byteProp;
	
	public boolean booleanProp;
	
	public float floatProp;
	
	public List<String> listProp;
	
	public Map<String, String> mapProp;
	
	public String stringPropWithColumn;
	
	public 	Timestamp timestampPropWithColumn;
	
	public 	int intPropWithColumn;
	
	public long longPropWithColumn;
	
	public double doublePropWithColumn;
	
	public byte bytePropWithColumn;
	
	public boolean booleanPropWithColumn;
	
	public float floatPropWithColumn;
	
	public List<String> listPropWithColumn;
	
	public Map<String, String> mapPropWithColumn;
	
	public Map<String, String> mapWithTablename;
	
	public Map<String, String> mapWithTablenameAndLazyLoaded;

	public Map<String, Map<String, String>> mapOfMaps;
	
	public String stringPropUmapped;
	
	public Timestamp timestampPropUmapped;
	
	public int intPropUmapped;
	
	public long longPropUmapped;
	
	public double doublePropUmapped;
	
	public 	byte bytePropUmapped;
	
	public boolean booleanPropUmapped;
	
	public List<String> listPropUmapped;
	
	public Map<String, String> mapPropUmapped;
	
	public MyBean() {
	}
	
	public MyBean(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getStringProp() {
		return stringProp;
	}

	public void setStringProp(String stringProp) {
		this.stringProp = stringProp;
	}

	public Timestamp getTimestampProp() {
		return timestampProp;
	}

	public void setTimestampProp(Timestamp timestampProp) {
		this.timestampProp = timestampProp;
	}

	public int getIntProp() {
		return intProp;
	}

	public void setIntProp(int intProp) {
		this.intProp = intProp;
	}

	public long getLongProp() {
		return longProp;
	}

	public void setLongProp(long longProp) {
		this.longProp = longProp;
	}

	public double getDoubleProp() {
		return doubleProp;
	}

	public void setDoubleProp(double doubleProp) {
		this.doubleProp = doubleProp;
	}

	public float getFloatProp() {
		return floatProp;
	}

	public void setFloatProp(float floatProp) {
		this.floatProp = floatProp;
	}

	public byte getByteProp() {
		return byteProp;
	}

	public void setByteProp(byte byteProp) {
		this.byteProp = byteProp;
	}

	public boolean isBooleanProp() {
		return booleanProp;
	}

	public void setBooleanProp(boolean booleanProp) {
		this.booleanProp = booleanProp;
	}

	public List<String> getListProp() {
		return listProp;
	}

	public void setListProp(List<String> listProp) {
		this.listProp = listProp;
	}

	public Map<String, String> getMapProp() {
		return mapProp;
	}

	public void setMapProp(Map<String, String> mapProp) {
		this.mapProp = mapProp;
	}

	public String getStringPropUmapped() {
		return stringPropUmapped;
	}

	public void setStringPropUmapped(String stringPropUmapped) {
		this.stringPropUmapped = stringPropUmapped;
	}

	public Timestamp getTimestampPropUmapped() {
		return timestampPropUmapped;
	}

	public void setTimestampPropUmapped(Timestamp timestampPropUmapped) {
		this.timestampPropUmapped = timestampPropUmapped;
	}

	public int getIntPropUmapped() {
		return intPropUmapped;
	}

	public void setIntPropUmapped(int intPropUmapped) {
		this.intPropUmapped = intPropUmapped;
	}

	public long getLongPropUmapped() {
		return longPropUmapped;
	}

	public void setLongPropUmapped(long longPropUmapped) {
		this.longPropUmapped = longPropUmapped;
	}

	public double getDoublePropUmapped() {
		return doublePropUmapped;
	}

	public void setDoublePropUmapped(double doublePropUmapped) {
		this.doublePropUmapped = doublePropUmapped;
	}

	public byte getBytePropUmapped() {
		return bytePropUmapped;
	}

	public void setBytePropUmapped(byte bytePropUmapped) {
		this.bytePropUmapped = bytePropUmapped;
	}

	public boolean isBooleanPropUmapped() {
		return booleanPropUmapped;
	}

	public void setBooleanPropUmapped(boolean booleanPropUmapped) {
		this.booleanPropUmapped = booleanPropUmapped;
	}

	public List<String> getListPropUmapped() {
		return listPropUmapped;
	}

	public void setListPropUmapped(List<String> listPropUmapped) {
		this.listPropUmapped = listPropUmapped;
	}

	public Map<String, String> getMapPropUmapped() {
		return mapPropUmapped;
	}

	public void setMapPropUmapped(Map<String, String> mapPropUmapped) {
		this.mapPropUmapped = mapPropUmapped;
	}

	public Map<String, String> getMapWithTablename() {
		return mapWithTablename;
	}

	public void setMapWithTablename(Map<String, String> mapWithTablename) {
		this.mapWithTablename = mapWithTablename;
	}

	public Map<String, String> getMapWithTablenameAndLazyLoaded() {
		return mapWithTablenameAndLazyLoaded;
	}

	public void setMapWithTablenameAndLazyLoaded(Map<String, String> mapWithTablenameAndLazyLoaded) {
		this.mapWithTablenameAndLazyLoaded = mapWithTablenameAndLazyLoaded;
	}

	public Map<String, Map<String, String>> getMapOfMaps() {
		return mapOfMaps;
	}

	public void setMapOfMaps(Map<String, Map<String, String>> mapOfMaps) {
		this.mapOfMaps = mapOfMaps;
	}

	public String getStringPropWithColumn() {
		return stringPropWithColumn;
	}

	public void setStringPropWithColumn(String stringPropWithColumn) {
		this.stringPropWithColumn = stringPropWithColumn;
	}

	public Timestamp getTimestampPropWithColumn() {
		return timestampPropWithColumn;
	}

	public void setTimestampPropWithColumn(Timestamp timestampPropWithColumn) {
		this.timestampPropWithColumn = timestampPropWithColumn;
	}

	public int getIntPropWithColumn() {
		return intPropWithColumn;
	}

	public void setIntPropWithColumn(int intPropWithColumn) {
		this.intPropWithColumn = intPropWithColumn;
	}

	public long getLongPropWithColumn() {
		return longPropWithColumn;
	}

	public void setLongPropWithColumn(long longPropWithColumn) {
		this.longPropWithColumn = longPropWithColumn;
	}

	public double getDoublePropWithColumn() {
		return doublePropWithColumn;
	}

	public void setDoublePropWithColumn(double doublePropWithColumn) {
		this.doublePropWithColumn = doublePropWithColumn;
	}

	public byte getBytePropWithColumn() {
		return bytePropWithColumn;
	}

	public void setBytePropWithColumn(byte bytePropWithColumn) {
		this.bytePropWithColumn = bytePropWithColumn;
	}

	public boolean isBooleanPropWithColumn() {
		return booleanPropWithColumn;
	}

	public void setBooleanPropWithColumn(boolean booleanPropWithColumn) {
		this.booleanPropWithColumn = booleanPropWithColumn;
	}

	public float getFloatPropWithColumn() {
		return floatPropWithColumn;
	}

	public void setFloatPropWithColumn(float floatPropWithColumn) {
		this.floatPropWithColumn = floatPropWithColumn;
	}

	public List<String> getListPropWithColumn() {
		return listPropWithColumn;
	}

	public void setListPropWithColumn(List<String> listPropWithColumn) {
		this.listPropWithColumn = listPropWithColumn;
	}

	public Map<String, String> getMapPropWithColumn() {
		return mapPropWithColumn;
	}

	public void setMapPropWithColumn(Map<String, String> mapPropWithColumn) {
		this.mapPropWithColumn = mapPropWithColumn;
	}
}
