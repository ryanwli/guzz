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
package org.guzz.dialect;

import org.guzz.id.IdentifierGenerationException;
import org.guzz.orm.type.BytesSQLDataType;
import org.guzz.orm.type.ClobSQLDataType;
import org.guzz.orm.type.StringSQLDataType;

/**
 * 
 * A dialect for Oracle 8i databases.
 *
 * @author liu kaixuan(liukaixuan@gmail.com)
 */
public class Oracle8iDialect extends AbstractDialect {
	
	public Oracle8iDialect(){
		super() ;
		
		this.registerUserDefinedTypes("Oracle.Long", StringSQLDataType.class) ;
		this.registerUserDefinedTypes("varchar2", StringSQLDataType.class) ;
		this.registerUserDefinedTypes("nclob", ClobSQLDataType.class) ;
		this.registerUserDefinedTypes("raw", BytesSQLDataType.class) ;
	}

	public String getLimitedString(String sql, int offset, int limit) {
		sql = sql.trim() ;
		String sql2 = sql.toLowerCase() ;
		
		boolean isForUpdate = false ;
		boolean isForUpdateNoWait = false ;
		
		if( sql2.endsWith(" for update") ){
			sql = sql.substring(0, sql.length() - 11) ;
			isForUpdate = true ;
		}else if( sql2.endsWith(" for update nowait") ){
			sql = sql.substring( 0, sql.length() - 18) ;
			isForUpdateNoWait = true ;
		}

		StringBuffer sb = new StringBuffer(sql.length() + 128) ;
		
		if(offset > 0){
			sb.append("select * from ( select row_.*, rownum rownum_ from ( ") ;
		}
		else{
			sb.append("select * from ( ") ;
		}
		
		sb.append(sql) ;
		
		if(offset > 0){
			sb.append(" ) row_ ) where rownum_ <= ").append(limit + offset).append(" and rownum_ > ").append(offset) ;
		}
		else{
			sb.append(" ) where rownum <= ").append(limit) ;
		}

		if(isForUpdate ) {
			sb.append( " for update" ) ;
		}else if(isForUpdateNoWait){
			sb.append( " for update nowait" ) ;
		}

		return sb.toString() ;
	}
	
	public String getSelectInsertedAutoIdClause() {
		return null ;
	}

	public String getSelectSequenceClause(String sequenceName) {
		if(sequenceName == null){
			throw new IdentifierGenerationException("sequence name cann't be null.") ;
		}
		
		StringBuffer sb = new StringBuffer(32) ;
		sb.append("select ").append(sequenceName).append(".nextval from dual") ;		
		
		return sb.toString() ;
	}

	public String getSelectGUIDClause() {
		return "select rawtohex(sys_guid()) from dual";
	}

	public boolean supportsSequence() {
		return true;
	}

	public String getNativeIDGenerator() {
		return "sequence";
	}

	public String getForUpdateNoWaitString(String sql) {
		return sql + " for update nowait";
	}

	public String getForUpdateString(String sql) {
		return sql + " for update";
	}
	
	public String getEscapedColunmName(String columnName) {
		return '"' + columnName + '"' ;
	}
	
	/**
	 * @return 10
	 */
	public int getDefaultBatchSize(){
		return 10 ;
	}
	
}
