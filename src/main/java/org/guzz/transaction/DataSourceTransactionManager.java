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
package org.guzz.transaction;

import org.guzz.connection.DBGroupManager;
import org.guzz.dao.WriteTemplate;
import org.guzz.orm.mapping.ObjectMappingManager;
import org.guzz.orm.sql.CompiledSQLBuilder;
import org.guzz.orm.sql.CompiledSQLManager;
import org.guzz.service.core.DebugService;

/**
 * 
 * 
 *
 * @author liukaixuan(liukaixuan@gmail.com)
 */
public class DataSourceTransactionManager implements TransactionManager {
	
	private CompiledSQLManager compiledSQLManager ;
	
	//static? no. allow mulit instances of web apps under one container.
	protected CompiledSQLBuilder compiledSQLBuilder ;
			
	private ObjectMappingManager omm ;
	
	private DebugService debugService ;
	
	private DBGroupManager dbGroupManager ;
	
	protected final TranSessionLocator tranSessionLocator ;
	
	public DataSourceTransactionManager(ObjectMappingManager omm, CompiledSQLManager compiledSQLManager,
			CompiledSQLBuilder compiledSQLBuilder, DebugService debugService, DBGroupManager dbGroupManager, 
			TranSessionLocator tranSessionLocator) {
		this.omm = omm ;
		this.compiledSQLManager = compiledSQLManager ;
		this.compiledSQLBuilder = compiledSQLBuilder ;
		this.debugService = debugService ;
		this.dbGroupManager = dbGroupManager ;
		this.tranSessionLocator = tranSessionLocator ;
	}
	
	public ReadonlyTranSession openDelayReadTran() {
		return new ReadonlyTranSessionImpl(omm, compiledSQLManager, debugService, dbGroupManager, true) ;
	}

	public WriteTranSession openRWTran(boolean autoCommit)  {
		return new WriteTranSessionImpl(omm, compiledSQLManager, debugService, dbGroupManager, autoCommit) ;
	}

	public ReadonlyTranSession openNoDelayReadonlyTran() {
		return new ReadonlyTranSessionImpl(omm, compiledSQLManager, debugService, dbGroupManager, false) ;
	}
	
	public CompiledSQLBuilder getCompiledSQLBuilder() {
		return compiledSQLBuilder;
	}

	public WriteTemplate createBindedWriteTemplate() {
		return tranSessionLocator.currentWriteTemplate() ;
	}

}
