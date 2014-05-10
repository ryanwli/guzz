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
package org.guzz.orm.type;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.guzz.Guzz;
import org.guzz.dialect.Dialect;
import org.guzz.exception.DataTypeException;
import org.guzz.pojo.lob.TranBlob;

/**
 * 
 * BLOB data type. the returned value is {@link TranBlob}.
 * 
 * @see TranBlob
 * @see Blob
 *
 * @author liu kaixuan(liukaixuan@gmail.com)
 */
public class BlobSQLDataType implements SQLDataType, DialectAware {
	
	private Dialect dialect ;

	public Object getSQLValue(ResultSet rs, String colName) throws SQLException {
		Blob c = rs.getBlob(colName) ;
		
		return c == null ? null : new TranBlob(c) ;
	}

	public Object getSQLValue(ResultSet rs, int colIndex) throws SQLException {
		Blob c = rs.getBlob(colIndex) ;
		
		return c == null ? null : new TranBlob(c) ;
	}

	public void setNullToValue(Object nullValue) {
		if(nullValue != null){
			throw new DataTypeException("null value unsupported. nullValue is:" + nullValue) ;
		}
	}

	public void setSQLValue(PreparedStatement pstm, int parameterIndex, Object value) throws SQLException {
		if(value instanceof String){
			value = getFromString((String) value) ;
		}
		
		if(value == null){
			pstm.setNull(parameterIndex, java.sql.Types.BLOB) ;
			return ;
		}
		
		boolean useStream = false ;
		Blob blob = (Blob) value ;
		
		if(blob instanceof TranBlob && !((TranBlob) blob).isLoadedFromDB() && this.dialect.useStreamToInsertLob()){
			useStream = true ;
		}
		
		if(useStream){
			pstm.setBinaryStream(parameterIndex, blob.getBinaryStream(), (int) blob.length() );
		}else{
			pstm.setBlob(parameterIndex, (Blob) value) ;
		}
	}
	
	public Class getDataType(){
		return Blob.class ;
	}

	public Object getFromString(String value) {
		if(value == null) return null ;
		
		return Guzz.createBlob(value.getBytes()) ;
	}

	public void setDialect(Dialect dialect) {
		this.dialect = dialect ;
	}

}
