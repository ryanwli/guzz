/*
 * Copyright 2008-2009 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.guzz.orm.rdms;

import java.sql.ResultSet;

import org.guzz.dao.PersistListener;
import org.guzz.dialect.Dialect;
import org.guzz.orm.ColumnDataLoader;
import org.guzz.orm.ColumnORM;
import org.guzz.orm.type.SQLDataType;

/**
 * 
 * 数据库表的一列元数据。用于构建数据库模型。
 *
 * @author liukaixuan(liukaixuan@gmail.com)
 */
public class TableColumn {
	
	private static final char ESCAPE_COL_CHAR = '`' ;
	
	private String colNameForSQL ;

	private String colNameForRS ;
	
	private String propName ;
	
	private boolean allowUpdate = true;
	
	private boolean allowInsert = true ;
	
	private String type ;
	
	private String nullValue ;
	
	private boolean lazy = false ;
		
	private ColumnORM orm ;
	
	private final Table table ;
	
	private final Dialect dialect ;
	
	public TableColumn(Table table){
		this.table = table ;
		this.dialect = table.getDialect() ;
	}
	
	public void setColName(String colName){
		int len = colName.length() ;
		
		if(colName.charAt(0) == ESCAPE_COL_CHAR && colName.charAt(len - 1) == ESCAPE_COL_CHAR){
			this.colNameForRS = colName.substring(1, len -1) ;
			this.colNameForSQL = dialect.getEscapedColunmName(this.colNameForRS) ;
		}else{
			this.colNameForRS = colName ;
			this.colNameForSQL = colName ;
		}
	}

	public String getPropName() {
		return propName;
	}

	public void setPropName(String propName) {
		this.propName = propName;
	}

	public boolean isAllowUpdate() {
		return allowUpdate;
	}

	public void setAllowUpdate(boolean allowUpdate) {
		this.allowUpdate = allowUpdate;
	}

	public boolean isAllowInsert() {
		return allowInsert;
	}

	public void setAllowInsert(boolean allowInsert) {
		this.allowInsert = allowInsert;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public boolean isLazy() {
		return lazy;
	}

	public void setLazy(boolean lazy) {
		this.lazy = lazy;
	}

	public ColumnDataLoader getDataLoader() {
		return orm.columnDataLoader;
	}

	public String getNullValue() {
		return nullValue;
	}

	public void setNullValue(String nullValue) {
		if("null".equals(nullValue)){
			nullValue = null ;
		}
		
		this.nullValue = nullValue;
		
		//后续修改
		if(orm != null && getSqlDataType() != null){
			getSqlDataType().setNullToValue(getSqlDataType().getFromString(nullValue)) ;
		}
	}

	public SQLDataType getSqlDataType() {
		return orm.sqlDataType;
	}

	public ColumnORM getOrm() {
		return orm;
	}

	public void setOrm(ColumnORM orm) {
		this.orm = orm;
		
		if(orm.columnDataLoader instanceof PersistListener){
			this.table.addPersistListener((PersistListener) orm.columnDataLoader) ;
		}
	}

	/**
	 * Column name for building sqls.
	 */
	public String getColNameForSQL() {
		return colNameForSQL;
	}

	/**
	 * Column name for retrieving value from {@link ResultSet}.
	 */
	public String getColNameForRS() {
		return colNameForRS;
	}
	
}
