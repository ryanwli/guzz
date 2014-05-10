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
package org.guzz.id;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Properties;

import org.guzz.dialect.Dialect;
import org.guzz.jdbc.JDBCTemplate;
import org.guzz.orm.mapping.POJOBasedObjectMapping;
import org.guzz.orm.rdms.TableColumn;
import org.guzz.orm.sql.SQLQueryCallBack;
import org.guzz.transaction.WriteTranSession;

/**
 * 
 * 
 *
 * @author liukaixuan(liukaixuan@gmail.com)
 */
public class AutoIncrementIdGenerator implements IdentifierGenerator, Configurable {
	private Dialect dialect ;
	private POJOBasedObjectMapping mapping ;
	private TableColumn pkColumn ;
	
	protected void setPrimaryKey(Object domainObject, Object value){
		mapping.getBeanWrapper().setValue(domainObject, pkColumn.getPropName(), value) ;
	}

	public Serializable preInsert(WriteTranSession session, Object domainObject, Object tableCondition) {
		return null ;
	}
	
	public Serializable postInsert(WriteTranSession session, Object domainObject, Object tableCondition) {
		JDBCTemplate jt = session.createJDBCTemplate(domainObject.getClass(), tableCondition) ;
		
		Object pk = jt.executeQuery(dialect.getSelectInsertedAutoIdClause(), 
				new SQLQueryCallBack(){

					public Object iteratorResultSet(ResultSet rs) throws Exception {
						if(rs.next()){
							return pkColumn.getSqlDataType().getSQLValue(rs, 1) ;
						}else{
							throw new IdentifierGenerationException("IdentifierGenerator is invalid.") ;
						}
					}
			
				}
		) ;
		
		setPrimaryKey(domainObject, pk) ;
		
		return (Serializable) pk ;
	}

	public boolean insertWithPKColumn() {
		return false;
	}
	
	public void configure(Dialect dialect, POJOBasedObjectMapping mapping, Properties params) {
		this.dialect = dialect ;
		this.mapping = mapping ;
		this.pkColumn = mapping.getTable().getPKColumn() ;
	}

}
