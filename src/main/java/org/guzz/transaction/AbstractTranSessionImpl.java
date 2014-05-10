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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.guzz.Guzz;
import org.guzz.connection.ConnectionFetcher;
import org.guzz.connection.DBGroup;
import org.guzz.connection.DBGroupManager;
import org.guzz.connection.PhysicsDBGroup;
import org.guzz.dao.PageFlip;
import org.guzz.dialect.Dialect;
import org.guzz.exception.DaoException;
import org.guzz.exception.GuzzException;
import org.guzz.exception.JDBCException;
import org.guzz.exception.ORMException;
import org.guzz.jdbc.JDBCTemplate;
import org.guzz.jdbc.JDBCTemplateImpl;
import org.guzz.orm.ObjectMapping;
import org.guzz.orm.mapping.ObjectMappingManager;
import org.guzz.orm.mapping.POJOBasedObjectMapping;
import org.guzz.orm.mapping.RowDataLoader;
import org.guzz.orm.rdms.Table;
import org.guzz.orm.se.SearchExpression;
import org.guzz.orm.se.SearchParams;
import org.guzz.orm.sql.BindedCompiledSQL;
import org.guzz.orm.sql.CompiledSQL;
import org.guzz.orm.sql.CompiledSQLBuilder;
import org.guzz.orm.sql.CompiledSQLManager;
import org.guzz.orm.sql.MarkedSQL;
import org.guzz.orm.sql.NormalCompiledSQL;
import org.guzz.orm.type.SQLDataType;
import org.guzz.pojo.GuzzProxy;
import org.guzz.service.core.DebugService;
import org.guzz.util.CloseUtil;
import org.guzz.util.javabean.BeanCreator;
import org.guzz.util.javabean.BeanWrapper;

/**
 * 
 * 
 *
 * @author liukaixuan(liukaixuan@gmail.com)
 */
public class AbstractTranSessionImpl {
	protected transient final Log log = LogFactory.getLog(getClass()) ;	
		
	protected final ObjectMappingManager omm ;
	
	protected final CompiledSQLManager compiledSQLManager ;
	
	protected JDBCTemplate jdbcTemplate ;
	
	protected final DebugService debugService ;
	
	protected final DBGroupManager dbGroupManager ;	
	
	protected boolean isReadonly ;
	
	protected final CompiledSQLBuilder compiledSQLBuilder ;
	
	protected final ConnectionsGroup connectionsGroup ;	

	private int queryTimeoutInSeconds ;
	
	public AbstractTranSessionImpl(ObjectMappingManager omm, CompiledSQLManager compiledSQLManager, ConnectionFetcher connectionFetcher, DebugService debugService, DBGroupManager dbGroupManager, boolean isReadonly) {
		this.omm = omm ;
		this.compiledSQLManager = compiledSQLManager ;
		this.debugService = debugService ;
		this.dbGroupManager = dbGroupManager ;
		this.isReadonly = isReadonly ;
		this.compiledSQLBuilder = compiledSQLManager.getCompiledSQLBuilder() ;
		this.connectionsGroup = new ConnectionsGroup(connectionFetcher) ;
	}	

	/**
	 * Construct a new TranSession using the same {@link ConnectionsGroup} of the given TranSession.
	 * 
	 * The newly created TranSesson will share the same transaction with the old one.
	 */
	public AbstractTranSessionImpl(AbstractTranSessionImpl sessionImpl) {
		this.omm = sessionImpl.omm ;
		this.compiledSQLManager = sessionImpl.compiledSQLManager ;
		this.debugService = sessionImpl.debugService ;
		this.dbGroupManager = sessionImpl.dbGroupManager ;
		this.isReadonly = sessionImpl.isReadonly ;
		this.compiledSQLBuilder = sessionImpl.compiledSQLBuilder ;
		this.connectionsGroup = sessionImpl.connectionsGroup ;
	}
	
	public Class getRealDomainClass(Object domainObject){
		if(domainObject instanceof GuzzProxy){
			return ((GuzzProxy) domainObject).getProxiedClass() ;
		}else{
			return domainObject.getClass() ;
		}
	}

	public void close() {
		this.connectionsGroup.close() ;
	}
	
	public Connection getConnection(DBGroup group, Object tableCondition){
		PhysicsDBGroup fdb = group.getPhysicsDBGroup(tableCondition) ;
		
		return getConnection(fdb) ;
	}
	
	protected Connection getConnection(PhysicsDBGroup fdb){
		return this.connectionsGroup.getConnection(fdb) ;
	}

	public IsolationsSavePointer setTransactionIsolation(int isolationLevel) {
		return connectionsGroup.setTransactionIsolation(isolationLevel) ;
	}

	public boolean isIsolationLevelChanged() {
		return connectionsGroup.isIsolationLevelChanged() ;
	}

	public void resetTransactionIsolationTo(IsolationsSavePointer savePointer) {
		connectionsGroup.resetTransactionIsolationTo(savePointer) ;
	}

	public void resetTransactionIsolationToLastSavePointer() {
		connectionsGroup.resetTransactionIsolationToLastSavePointer() ;
	}
	
	public List list(String id, Map params){
		return list(id, params, 1, Integer.MAX_VALUE) ;
	}
	
	public List list(String id, Map params, int startPos, int maxSize){
		CompiledSQL sql = compiledSQLManager.getSQL(id) ;
		if(sql == null){
			throw new GuzzException("sql :[" + id + "] not found.") ;
		}
		
		return list(sql.bind(params), startPos, maxSize);
	}
	
	/**
	 * Execute query without pagination.
	 * 
	 * @param bsql
	 **/
	public List list(BindedCompiledSQL bsql){
		return list(bsql, 1, Integer.MAX_VALUE) ;
	}

	/**
	 * @param bsql
	 * @param startPos 从1开始
	 * @param maxSize
	 **/
	public List list(BindedCompiledSQL bsql, int startPos, int maxSize) {
		ObjectMapping m = bsql.getCompiledSQLToRun().getMapping() ;
		String rawSQL = bsql.getSQLToRun() ;
		if(m == null){
			throw new ORMException("ObjectMapping is null. sql is:" + rawSQL) ;
		}
		
		RowDataLoader loader = bsql.getRowDataLoader() ;		
		
		DBGroup db = m.getDbGroup() ;		
		Dialect dialect = m.getDbGroup().getDialect() ;

		//强制锁机制
		LockMode lock = bsql.getLockMode() ;
		
		if(lock == LockMode.UPGRADE){
			rawSQL = dialect.getForUpdateString(rawSQL) ;
		}else if(lock == LockMode.UPGRADE_NOWAIT){
			rawSQL = dialect.getForUpdateNoWaitString(rawSQL) ;
		}
		
		//TODO: check if the defaultDialect supports prepared bind in limit clause, and put the limit to compiledSQL
				
		//add limit clause.	
		if(!(startPos == 1 && maxSize == Integer.MAX_VALUE)){
			rawSQL = db.getDialect().getLimitedString(rawSQL, startPos - 1, maxSize) ;
		}
		
		boolean measureTime = this.debugService.isMeasureTime() ;
		long startTime = 0L ;
		if(measureTime){
			startTime = System.nanoTime() ;
		}
		
		PreparedStatement pstm = null ;
		ResultSet rs = null ;
		
		try{
			Connection conn = getConnection(db, bsql.getTableCondition()) ;
			pstm = conn.prepareStatement(rawSQL) ;
			this.applyQueryTimeout(pstm) ;
			
			bsql.prepareNamedParams(db.getDialect(), pstm) ;
			
			rs = pstm.executeQuery() ;
			
			if(this.debugService.isLogSQL()){
				long timeCost = 0 ;
				if(measureTime){
					timeCost = System.nanoTime() - startTime ;
				}
				
				this.debugService.logSQL(bsql, rawSQL, timeCost) ;
			}
			
			//do ORM
			LinkedList results = new LinkedList() ;
			
			while(rs.next()){
				if(loader == null){
					results.addLast(m.rs2Object(rs, bsql.getResultClass())) ;
				}else{
					results.addLast(loader.rs2Object(m, rs)) ;
				}
			}
			
			return results ;
		}catch(SQLException e){
			throw new JDBCException("Error Code:" + e.getErrorCode() + ", sql:" + rawSQL, e, e.getSQLState()) ;
		}finally{
			CloseUtil.close(rs) ;
			CloseUtil.close(pstm) ;
		}
	}
	
	public List list(SearchExpression se) {
		if(se.isEmptyQuery()){
			//must resulted in no results.
			return new LinkedList() ;
		}
		
		ObjectMapping m = omm.getObjectMapping(se.getFrom(), se.getTableCondition()) ;
		
		if(m == null){
			throw new ORMException("unknow object:" + se.getFrom()) ;
		}
		
		SearchParams sp = new SearchParams() ;
		MarkedSQL ms = se.toLoadRecordsMarkedSQL((POJOBasedObjectMapping) m, sp) ;
		
		CompiledSQL sql = this.compiledSQLBuilder.buildCompiledSQL(ms).setParamPropMapping(sp.getParamPropMapping()) ;
		
		return list(se.prepareHits(sql.bind(sp.getSearchParams())), se.getStartPos(), se.getPageSize()) ;
	}
	
	public long count(SearchExpression se) {
		if(se.isEmptyQuery()){
			//must resulted in no results.
			return 0L ;
		}
		
		ObjectMapping m = omm.getObjectMapping(se.getFrom(), se.getTableCondition()) ;
		
		if(m == null){
			throw new ORMException("unknown business:" + se.getFrom()) ;
		}
		
		SearchParams sp = new SearchParams() ;
		
		MarkedSQL ms = se.toComputeRecordNumberSQL((POJOBasedObjectMapping) m, sp) ;
		
		CompiledSQL sql = this.compiledSQLBuilder.buildCompiledSQL(ms).setParamPropMapping(sp.getParamPropMapping()) ;
		
		Object ret = findCell00(se.prepareHits(sql.bind(sp.getSearchParams())), Long.class.getName()) ;
		
		if(ret == null){
			return 0L ;
		}else{
			return ((Long) ret).longValue() ;
		}
	}
	
	public PageFlip page(SearchExpression se) {
		if(se.isEmptyQuery()){
			Class m_flip = se.getPageFlipClass() ;
			
			PageFlip pf = null ;
			if(m_flip == null){
				pf = new PageFlip() ;
			}else{
				pf = (PageFlip) BeanCreator.newBeanInstance(m_flip) ;
			}
			
			//must resulted in no results.
			pf.setResult(0, se.getPageNo(), se.getPageSize(), new LinkedList()) ;
			
			return pf;
		}
		
		ObjectMapping m = omm.getObjectMapping(se.getFrom(), se.getTableCondition()) ;
		
		if(m == null){
			throw new ORMException("unknow object:" + se.getFrom()) ;
		}
		
		PageFlip pf = null ;
		
		Class m_flip = se.getPageFlipClass() ;
		if(m_flip == null){
			pf = new PageFlip() ;
		}else{
			pf = (PageFlip) BeanCreator.newBeanInstance(m_flip) ;
		}
		
		List records = null ;
		
		if(se.isLoadRecords()){
			records = list(se) ;
		}
		
		int recordCount = 0 ;
		
		if(se.isComputeRecordNumber()){
			SearchParams sp = new SearchParams() ;
			MarkedSQL ms = se.toComputeRecordNumberSQL((POJOBasedObjectMapping) m, sp) ;
			
			CompiledSQL sql = this.compiledSQLBuilder.buildCompiledSQL(ms).setParamPropMapping(sp.getParamPropMapping()) ;
			
			Integer count = (Integer) findCell00(se.prepareHits(sql.bind(sp.getSearchParams())), "int") ;
			recordCount = count.intValue() ;
		}
		
		pf.setResult(recordCount, se.getPageNo(), se.getPageSize(), records) ;
		
		return pf;
	}

	public Object findCell00(String id, Map params, String returnType){
		CompiledSQL sql = compiledSQLManager.getSQL(id) ;
		if(sql == null){
			throw new GuzzException("sql :[" + id + "] not found.") ;
		}
		
		return findCell00(sql.bind(params), returnType);
	}

	public Object findCell00(BindedCompiledSQL bsql, String returnType){
		ObjectMapping m = bsql.getCompiledSQLToRun().getMapping() ;
		String rawSQL = bsql.getSQLToRun() ;
		if(m == null){
			throw new ORMException("ObjectMapping is null. sql is:" + rawSQL) ;
		}
		
		Dialect dialect = m.getDbGroup().getDialect() ;

		//强制锁机制
		LockMode lock = bsql.getLockMode() ;
		
		if(lock == LockMode.UPGRADE){
			rawSQL = dialect.getForUpdateString(rawSQL) ;
		}else if(lock == LockMode.UPGRADE_NOWAIT){
			rawSQL = dialect.getForUpdateNoWaitString(rawSQL) ;
		}
		
		RowDataLoader loader = bsql.getRowDataLoader() ;
		DBGroup db = m.getDbGroup() ;
				
		boolean measureTime = this.debugService.isMeasureTime() ;
		long startTime = 0L ;
		if(measureTime){
			startTime = System.nanoTime() ;
		}
		
		PreparedStatement pstm = null ;
		ResultSet rs = null ;
		
		try{
			Connection conn = getConnection(db, bsql.getTableCondition()) ;
			pstm = conn.prepareStatement(rawSQL) ;
			this.applyQueryTimeout(pstm) ;
			bsql.prepareNamedParams(db.getDialect(), pstm) ;
			
			rs = pstm.executeQuery() ;
			
			if(this.debugService.isLogSQL()){
				long timeCost = 0 ;
				if(measureTime){
					timeCost = System.nanoTime() - startTime ;
				}
				
				this.debugService.logSQL(bsql, rawSQL, timeCost) ;
			}
			
			if(rs.next()){
				if(loader != null){
					return loader.rs2Object(m, rs) ;
				}else if(returnType != null){
					SQLDataType type = db.getDialect().getDataType(returnType) ;
							
					return type.getSQLValue(rs, 1) ;
				}else{
					return rs.getObject(1) ;
				}
			}else{
				if(bsql.isExceptionOnNoRecordFound()){
					throw new DaoException("record not found for the query:[" + rawSQL + "], params:[" + bsql.getBindedParams() + "].") ;
				}else{
					return null ;
				}
			}
		}catch(SQLException e){
			throw new JDBCException("Error Code:" + e.getErrorCode() + ", sql:" + rawSQL, e, e.getSQLState()) ;
		}finally{
			CloseUtil.close(rs) ;
			CloseUtil.close(pstm) ;
		}
	}
	
	/**
	 * @param bsql
	 * @param returnType
	 */
	protected Object findCell00(BindedCompiledSQL bsql, SQLDataType returnType){
		ObjectMapping m = bsql.getCompiledSQLToRun().getMapping() ;
		String rawSQL = bsql.getSQLToRun() ;
		if(m == null){
			throw new ORMException("ObjectMapping is null. sql is:" + rawSQL) ;
		}
		
		Dialect dialect = m.getDbGroup().getDialect() ;

		//强制锁机制
		LockMode lock = bsql.getLockMode() ;
		
		if(lock == LockMode.UPGRADE){
			rawSQL = dialect.getForUpdateString(rawSQL) ;
		}else if(lock == LockMode.UPGRADE_NOWAIT){
			rawSQL = dialect.getForUpdateNoWaitString(rawSQL) ;
		}
		
		RowDataLoader loader = bsql.getRowDataLoader() ;
		DBGroup db = m.getDbGroup() ;
		
		boolean measureTime = this.debugService.isMeasureTime() ;
		long startTime = 0L ;
		if(measureTime){
			startTime = System.nanoTime() ;
		}
		
		PreparedStatement pstm = null ;
		ResultSet rs = null ;
		
		try{
			Connection conn = getConnection(db, bsql.getTableCondition()) ;
			pstm = conn.prepareStatement(rawSQL) ;
			this.applyQueryTimeout(pstm) ;
			bsql.prepareNamedParams(db.getDialect(), pstm) ;
			
			rs = pstm.executeQuery() ;
			
			if(this.debugService.isLogSQL()){
				long timeCost = 0 ;
				if(measureTime){
					timeCost = System.nanoTime() - startTime ;
				}
				
				this.debugService.logSQL(bsql, rawSQL, timeCost) ;
			}
			
			if(rs.next()){
				if(loader != null){
					return loader.rs2Object(m, rs) ;
				}else if(returnType != null){
					return returnType.getSQLValue(rs, 1) ;
				}else{
					return rs.getObject(1) ;
				}
			}else{
				if(bsql.isExceptionOnNoRecordFound()){
					throw new DaoException("record not found for the query:[" + rawSQL + "], params:[" + bsql.getBindedParams() + "].") ;
				}else{
					return null ;
				}
			}
		}catch(SQLException e){
			throw new JDBCException("Error Code:" + e.getErrorCode() + ", sql:" + rawSQL, e, e.getSQLState()) ;
		}finally{
			CloseUtil.close(rs) ;
			CloseUtil.close(pstm) ;
		}
	}

	public Object findObject(String id, Map params){
		CompiledSQL sql = compiledSQLManager.getSQL(id) ;
		if(sql == null){
			throw new GuzzException("sql :[" + id + "] not found.") ;
		}
		
		return findObject(sql.bind(params)) ;
	}

	public Object findObject(BindedCompiledSQL bsql) {
		ObjectMapping m = bsql.getCompiledSQLToRun().getMapping() ;
		String rawSQL = bsql.getSQLToRun() ;
		
		if(m == null){
			throw new ORMException("ObjectMapping is null. sql is:" + rawSQL) ;
		}
		
		RowDataLoader loader = bsql.getRowDataLoader() ;
		
		DBGroup db = m.getDbGroup() ;
		Dialect dialect = db.getDialect() ;
		
		//强制锁机制
		LockMode lock = bsql.getLockMode() ;
		
		if(lock == LockMode.UPGRADE){
			rawSQL = dialect.getForUpdateString(rawSQL) ;
		}else if(lock == LockMode.UPGRADE_NOWAIT){
			rawSQL = dialect.getForUpdateNoWaitString(rawSQL) ;
		}
		
		//TODO: check if the defaultDialect supports prepared bind in limit clause, and put the limit to compiledSQL
		rawSQL = db.getDialect().getLimitedString(rawSQL, 0, 1) ;
		
		boolean measureTime = this.debugService.isMeasureTime() ;
		long startTime = 0L ;
		if(measureTime){
			startTime = System.nanoTime() ;
		}
		
		PreparedStatement pstm = null ;
		ResultSet rs = null ;
		
		try{
			Connection conn = getConnection(db, bsql.getTableCondition()) ;
			pstm = conn.prepareStatement(rawSQL) ;
			this.applyQueryTimeout(pstm) ;
			
			bsql.prepareNamedParams(db.getDialect(), pstm) ;
			
			rs = pstm.executeQuery() ;
			
			if(this.debugService.isLogSQL()){
				long timeCost = 0 ;
				if(measureTime){
					timeCost = System.nanoTime() - startTime ;
				}
				
				this.debugService.logSQL(bsql, rawSQL, timeCost) ;
			}
			
			//do ORM		
			if(rs.next()){
				if(loader == null){
					return m.rs2Object(rs, bsql.getResultClass()) ;
				}else{
					return loader.rs2Object(m, rs) ;
				}
			}else{
				if(bsql.isExceptionOnNoRecordFound()){
					throw new DaoException("record not found for the query:[" + rawSQL + "], params:[" + bsql.getBindedParams() + "].") ;
				}else{
					return null ;
				}
			}
		}catch(SQLException e){
			throw new JDBCException("Error Code:" + e.getErrorCode() + ", sql:" + rawSQL, e, e.getSQLState()) ;
		}finally{
			CloseUtil.close(rs) ;
			CloseUtil.close(pstm) ;
		}
	}

	public Object findObject(SearchExpression se) {
		se.setPageNo(1) ;
		se.setPageSize(1) ;
		
		List l = list(se) ;
		if(l.isEmpty()){
			return null ;
		}else{
			return l.get(0) ;
		}
	}
	
	public Object findObjectByPK(String businessName, Serializable pk){
		CompiledSQL cs = this.compiledSQLManager.getDefinedSelectSQL(businessName) ;
		if(cs == null){
			throw new DaoException("no defined sql found for class:[" + businessName + "]. forget to register it in guzz.xml?") ;
		}
		
		BindedCompiledSQL bsql = cs.bindNoParams() ;
		String[] orderedParams = bsql.getCompiledSQLToRun().getOrderedParams() ;
		
		if(orderedParams.length != 1){
			throw new DaoException("error orm! too many params in findObjectByPK. class is:" + businessName) ;
		}
		
		return findObject(bsql.bind(orderedParams[0], pk)) ;
	}
	
	public Object findObjectByPK(Class domainClass, Serializable pk){
		return findObjectByPK(domainClass.getName(), pk) ;
	}
	
	public Object findObjectByPK(Class domainClass, int pk){
		return findObjectByPK(domainClass.getName(), new Integer(pk)) ;
	}
	
	public Object refresh(Object domainObject, LockMode lockMode){
		String className = getRealDomainClass(domainObject).getName() ;
		CompiledSQL cs = this.compiledSQLManager.getDefinedSelectSQL(className) ;
		if(cs == null){
			throw new DaoException("no defined sql found for class:[" + className + "]. forget to register it in guzz.xml?") ;
		}
		
		BindedCompiledSQL bsql = cs.bindNoParams() ;
		
		NormalCompiledSQL runtimeCS = bsql.getCompiledSQLToRun() ;
		POJOBasedObjectMapping mapping = (POJOBasedObjectMapping) runtimeCS.getMapping() ;
		Table table = mapping.getTable() ;
		
		BeanWrapper bw = mapping.getBeanWrapper() ;
		Object pk = bw.getValueUnderProxy(domainObject, table.getPKPropName()) ;
		String[] orderedParams = runtimeCS.getOrderedParams() ;
		
		if(orderedParams.length != 1){
			throw new DaoException("error orm! too many params in findObjectByPK. class is:" + className) ;
		}
		
		bsql.bind(orderedParams[0], pk).setLockMode(lockMode) ;
		
		//record must be exsit on refresh(), or raise a exception.
		bsql.setExceptionOnNoRecordFound(true) ;
		
		return findObject(bsql) ;
	}

	public JDBCTemplate createJDBCTemplate(Class domainClass) {
		return this.createJDBCTemplate(domainClass, Guzz.getTableCondition()) ;
	}
	
	public JDBCTemplate createJDBCTemplate(String businessName) {
		return this.createJDBCTemplate(businessName, Guzz.getTableCondition()) ;
	}
	
	public JDBCTemplate createJDBCTemplateByDbGroup(String dbGroup) {
		return this.createJDBCTemplateByDbGroup(dbGroup, Guzz.getTableCondition()) ;
	}

	public JDBCTemplate createJDBCTemplate(Class domainClass, Object tableCondition) {				
		return createJDBCTemplate(domainClass.getName(), tableCondition) ;
	}

	public JDBCTemplate createJDBCTemplateByDbGroup(String groupName, Object tableCondition) {
		DBGroup group = this.dbGroupManager.getGroup(groupName) ;
		
		Connection conn = getConnection(group, tableCondition) ;
		
		return new JDBCTemplateImpl(this, group.getDialect(), debugService, conn, isReadonly) ;
	}
	
	public JDBCTemplate createJDBCTemplate(String businessName, Object tableCondition){
		ObjectMapping map = this.omm.getObjectMapping(businessName, tableCondition) ;
		
		if(map == null){
			throw new ORMException("unknown business:[" + businessName + "]") ;
		}
		
		DBGroup group = map.getDbGroup() ;
		Connection conn = getConnection(group, tableCondition) ;
				
		return new JDBCTemplateImpl(this, group.getDialect(), debugService, conn, isReadonly) ;
	}
	
	/**
	 * Apply the current query timeout, if any, to the current <code>PreparedStatement</code>.
	 * 
	 * @param pstm PreparedStatement
	 * @see java.sql.PreparedStatement#setQueryTimeout
	 */
	public void applyQueryTimeout(PreparedStatement pstm){
		if(hasQueryTimeout()){
			try {
				pstm.setQueryTimeout(getQueryTimeoutInSeconds()) ;
			} catch (SQLException e) {
				throw new JDBCException("failed to setQueryTimeout to :" + getQueryTimeoutInSeconds(), e, e.getSQLState()) ;
			}
		}
	}
	
	/**
	 * Apply the current query timeout, if any, to the current <code>Statement</code>.
	 * 
	 * @param stmt Statement
	 * @see java.sql.Statement#setQueryTimeout
	 */
	public void applyQueryTimeout(Statement stmt){
		if(hasQueryTimeout()){
			try {
				stmt.setQueryTimeout(getQueryTimeoutInSeconds()) ;
			} catch (SQLException e) {
				throw new JDBCException("failed to setQueryTimeout to :" + getQueryTimeoutInSeconds(), e, e.getSQLState()) ;
			}
		}
	}
	
	/**
	 * Apply the current query timeout, if any, to the current <code>java.sql.CallableStatement</code>.
	 * 
	 * @param cs java.sql.CallableStatement
	 * @see java.sql.CallableStatement#setQueryTimeout
	 */
	public void applyQueryTimeout(java.sql.CallableStatement cs){
		if(hasQueryTimeout()){
			try {
				cs.setQueryTimeout(getQueryTimeoutInSeconds()) ;
			} catch (SQLException e) {
				throw new JDBCException("failed to setQueryTimeout to :" + getQueryTimeoutInSeconds(), e, e.getSQLState()) ;
			}
		}
	}
	
	public final boolean hasQueryTimeout(){
		return this.queryTimeoutInSeconds > 0 ;
	}

	public final int getQueryTimeoutInSeconds() {
		return this.queryTimeoutInSeconds;
	}

	public final void setQueryTimeoutInSeconds(int queryTimeoutInSeconds) {
		this.queryTimeoutInSeconds = queryTimeoutInSeconds;
	}

	public ConnectionsGroup getConnectionsGroup() {
		return this.connectionsGroup;
	}

}
