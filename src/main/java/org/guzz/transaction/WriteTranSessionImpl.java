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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.guzz.Guzz;
import org.guzz.bytecode.LazyPropChangeDetector;
import org.guzz.connection.ConnectionFetcher;
import org.guzz.connection.DBGroup;
import org.guzz.connection.DBGroupManager;
import org.guzz.connection.PhysicsDBGroup;
import org.guzz.dao.PersistListener;
import org.guzz.exception.DaoException;
import org.guzz.exception.GuzzException;
import org.guzz.exception.JDBCException;
import org.guzz.exception.ORMException;
import org.guzz.id.IdentifierGenerator;
import org.guzz.jdbc.ObjectBatcher;
import org.guzz.jdbc.ObjectBatcherImpl;
import org.guzz.jdbc.SQLBatcher;
import org.guzz.jdbc.SQLBatcherImpl;
import org.guzz.orm.ObjectMapping;
import org.guzz.orm.mapping.ObjectMappingManager;
import org.guzz.orm.mapping.POJOBasedObjectMapping;
import org.guzz.orm.rdms.Table;
import org.guzz.orm.rdms.TableColumn;
import org.guzz.orm.se.SearchExpression;
import org.guzz.orm.se.SearchParams;
import org.guzz.orm.sql.BindedCompiledSQL;
import org.guzz.orm.sql.CompiledSQL;
import org.guzz.orm.sql.CompiledSQLManager;
import org.guzz.orm.sql.MarkedSQL;
import org.guzz.orm.sql.NormalCompiledSQL;
import org.guzz.pojo.DynamicUpdatable;
import org.guzz.service.core.DatabaseService;
import org.guzz.service.core.DebugService;
import org.guzz.util.ArrayUtil;
import org.guzz.util.CloseUtil;
import org.guzz.util.javabean.BeanWrapper;

/**
 * 
 * 
 *
 * @author liukaixuan(liukaixuan@gmail.com)
 */
public class WriteTranSessionImpl extends AbstractTranSessionImpl implements WriteTranSession {
	private List psForBatch = null ;
	
	private List objectBatchers = null ;
	
	private ReadonlyTranSession read ;

	public WriteTranSessionImpl(ObjectMappingManager omm, CompiledSQLManager compiledSQLManager, DebugService debugService, DBGroupManager dbGroupManager, boolean autoCommit) {
		super(omm, compiledSQLManager, new WriteConnectionFetcher(autoCommit), debugService, dbGroupManager, false);
	}
	
	public boolean delete(Object domainObject) {
		return this.delete(domainObject, Guzz.getTableCondition()) ;
	}

	public Serializable insert(Object domainObject) {
		return this.insert(domainObject, Guzz.getTableCondition()) ;
	}

	public boolean update(Object domainObject) {
		return this.update(domainObject, Guzz.getTableCondition()) ;
	}
	
	public boolean delete(Object domainObject, Object tableCondition) {
		CompiledSQL cs = this.compiledSQLManager.getDefinedDeleteSQL(getRealDomainClass(domainObject).getName()) ;
		if(cs == null){
			throw new DaoException("no defined sql found for class:[" + getRealDomainClass(domainObject).getName() + "]. forget to register it in guzz.xml?") ;
		}
		
		BindedCompiledSQL bsql = cs.bindNoParams() ;
		bsql.setTableCondition(tableCondition) ;
		NormalCompiledSQL runtimeCS = bsql.getCompiledSQLToRun() ;
		
		POJOBasedObjectMapping mapping = (POJOBasedObjectMapping) runtimeCS.getMapping() ;
		PersistListener[] pls = mapping.getTable().getPersistListeners() ;
		
		BeanWrapper bw = mapping.getBeanWrapper() ;
		String[] props = runtimeCS.getOrderedParams() ;
		for(int i = 0 ; i < props.length ; i++){
			Object value = bw.getValueUnderProxy(domainObject, props[i]) ;
			bsql.bind(props[i], value) ;
		}
		
		boolean success = executeUpdateWithPrePL(runtimeCS.getMapping(), bsql, pls, domainObject, null, 3) == 1 ;

		if(pls.length > 0){
			Connection conn = getConnection(bsql.getPhysicsDBGroup()) ;
			for(int i = 0 ; i < pls.length ; i++){
				pls[i].postDelete(this, conn, domainObject) ;
			}
		}
		
		return success ;
	}

	public Serializable insert(Object domainObject, Object tableCondition) {
		//inserted domainObject must be orginal(not proxied).
		CompiledSQL cs = this.compiledSQLManager.getDefinedInsertSQL(domainObject.getClass().getName()) ;
		if(cs == null){
			throw new DaoException("no defined sql found for class:[" + domainObject.getClass().getName() + "]. forget to register it in guzz.xml?") ;
		}
		
		BindedCompiledSQL bsql = cs.bindNoParams() ;
		bsql.setTableCondition(tableCondition) ;
		NormalCompiledSQL runtimeCS = bsql.getCompiledSQLToRun() ;
		
		POJOBasedObjectMapping mapping = (POJOBasedObjectMapping) runtimeCS.getMapping() ;
		PersistListener[] pls = mapping.getTable().getPersistListeners() ;
		
		IdentifierGenerator ig = mapping.getTable().getIdentifierGenerator() ;		
		BeanWrapper bw = mapping.getBeanWrapper() ;
		
		Serializable pk = ig.preInsert(this, domainObject, bsql.getTableCondition()) ;
		
		String[] props = runtimeCS.getOrderedParams() ;
		for(int i = 0 ; i < props.length ; i++){
			Object value = bw.getValue(domainObject, props[i]) ;
			bsql.bind(props[i], value) ;
		}
		
		executeUpdateWithPrePL(runtimeCS.getMapping(), bsql, pls, domainObject, pk, 1) ;
		
		if(pk == null){
			pk = ig.postInsert(this, domainObject, bsql.getTableCondition()) ;
		}else{
			ig.postInsert(this, domainObject, bsql.getTableCondition()) ;
		}

		if(pls.length > 0){
			Connection conn = getConnection(bsql.getPhysicsDBGroup()) ;
			for(int i = 0 ; i < pls.length ; i++){
				pls[i].postInsert(this, conn, domainObject, pk) ;
			}
		}
		
		return pk ;
	}

	public boolean update(Object domainObject, Object tableCondition) {
		/*
		 * 1. 所有含有loader字段的属性不进行保存。
		 * 2. 所有lazy属性，只有在显式的调用setxxx方法后才进行保存。
		 * 3. 如果设置了dynamic-update=true，只更新显式调用setXXX的属性（lazy属性的变化也包含在此，因此dynamic-update检测到的更改字段包含lazy的属性的更改）。
		 */
		String domainClassName = getRealDomainClass(domainObject).getName() ;
		POJOBasedObjectMapping mapping = (POJOBasedObjectMapping) this.omm.getObjectMapping(domainClassName, tableCondition) ;
		
		if(mapping == null){
			throw new DaoException("ObjectMapping is null. class is:" + domainClassName) ;
		}
		
		BeanWrapper bw = mapping.getBeanWrapper() ;
		Table table = mapping.getBusiness().getTable() ;
		CompiledSQL cs = null ;
		String[] updatedProps = null ;
				
		boolean dynamicUpdateEnable = domainObject instanceof DynamicUpdatable ;
		
		//TODO:设计错误！为了让项目更灵活的控制字段更新，lazy的字段应该和非lazy字段分开计数。DynamicUpdatable应该 只 统计非lazy字段的修改情况。
		
		//dynamic-update=true
		if(dynamicUpdateEnable){
			String[] changedProps = ((DynamicUpdatable) domainObject).getChangedProps() ;
			
			if(changedProps == null){
				//null means: ignore dynamic-update=true.
				updatedProps = table.getPropsForUpdate() ;
			}else if(changedProps.length == 0){
				if(log.isDebugEnabled()){
					log.debug("changedProps is empty. the update operation is ignored. object is:" + domainObject) ;
				}
				
				return false ;
			}else{//update the changed properties.
				updatedProps = changedProps ;
				cs = this.compiledSQLManager.buildUpdateSQL(mapping, updatedProps) ;
				
				//reset the recorded changed properties.
				((DynamicUpdatable) domainObject).resetChangeCounter() ;
			}
		}else{
			updatedProps = table.getPropsForUpdate() ;
		}
		
		//has lazy properties
		if(!dynamicUpdateEnable && domainObject instanceof LazyPropChangeDetector){
			String[] changedProps = ((LazyPropChangeDetector) domainObject).getChangedLazyProps() ;
			
			if(changedProps != null && changedProps.length > 0){
				//add the changed props to update list.
				updatedProps = (String[]) ArrayUtil.addToArray(updatedProps, changedProps) ;
				cs = this.compiledSQLManager.buildUpdateSQL(mapping, updatedProps) ;
				
				//reset the recorded changed lazy-properties.
				((LazyPropChangeDetector) domainObject).resetLazyCounter() ;
			}
		}
		
		//update as normal(no dynamic, no lazy load)
		if(cs == null){
			cs = this.compiledSQLManager.getDefinedUpdateSQL(domainClassName) ;
			if(cs == null){
				throw new DaoException("no defined sql found for class:[" + domainClassName + "]. forget to register it in guzz.xml?") ;
			}
			//updateProps is already setted.
		}else{ //add pk property for where clause binding.
			updatedProps = (String[]) ArrayUtil.addToArray(updatedProps, table.getPKPropName()) ;
		}
		
		BindedCompiledSQL bsql = cs.bindNoParams() ;
		bsql.setTableCondition(tableCondition) ;
				
		for(int i = 0 ; i < updatedProps.length ; i++){
			Object value = bw.getValueUnderProxy(domainObject, updatedProps[i]) ;
			bsql.bind(updatedProps[i], value) ;
		}
		
		PersistListener[] pls = mapping.getTable().getPersistListeners() ;
		
		boolean success = executeUpdateWithPrePL(mapping, bsql, pls, domainObject, null, 2) == 1 ;

		if(pls.length > 0){
			Connection conn = getConnection(bsql.getPhysicsDBGroup()) ;
			for(int i = 0 ; i < pls.length ; i++){
				pls[i].postUpdate(this, conn, domainObject) ;
			}
		}
		
		return success ;
	}
	
	public int executeUpdate(String id, Map params){
		CompiledSQL cs = compiledSQLManager.getSQL(id) ;
		if(cs == null){
			throw new DaoException("configured sql not found. id is:" + id) ;
		}
		
		return executeUpdate(cs.bind(params)) ;
	}
	

	public int delete(SearchExpression se) {
		if(se.isEmptyQuery()){
			//must resulted in no results.
			return 0 ;
		}
		
		ObjectMapping m = omm.getObjectMapping(se.getFrom(), se.getTableCondition()) ;
		
		if(m == null){
			throw new ORMException("unknown business:" + se.getFrom()) ;
		}
		
		SearchParams sp = new SearchParams() ;
		MarkedSQL ms = se.toDeleteRecordString((POJOBasedObjectMapping) m, sp) ;
		CompiledSQL sql = this.compiledSQLBuilder.buildCompiledSQL(ms).setParamPropMapping(sp.getParamPropMapping()) ;
		
		return this.executeUpdate(se.prepareHits(sql.bind(sp.getSearchParams()))) ;
	}
	
	/**
	 * invoke {@link PersistListener} 's preXXX() before executeUpdate.
	 * 
	 * @param mapping runtime ObjectMapping
	 * @param bsql
	 * @param pls PersistListeners registered.
	 * @param domainObject
	 * @param pk primary key. available in inserting method.
	 * @param operation 1:insert, 2:update, 3:delete
	 */
	protected int executeUpdateWithPrePL(ObjectMapping mapping, BindedCompiledSQL bsql, PersistListener[] pls, Object domainObject, Serializable pk, int operation){
		String rawSQL = bsql.getSQLToRun() ;
		DBGroup db = mapping.getDbGroup() ;
		PreparedStatement pstm = null;
		
		boolean measureTime = this.debugService.isMeasureTime() ;
		long startTime = 0L ;
		if(measureTime){
			startTime = System.nanoTime() ;
		}
		
		try {
			Connection conn = getConnection(db, bsql.getTableCondition()) ;
			pstm = conn.prepareStatement(rawSQL);		
			this.applyQueryTimeout(pstm) ;	
			bsql.prepareNamedParams(db.getDialect(), pstm) ;
			
			if(pls.length > 0){
				for(int i = 0 ; i < pls.length ; i++){
					if(operation == 1){
						pls[i].preInsert(this, conn, pstm, bsql, domainObject, pk) ;
					}else if(operation == 2){
						pls[i].preUpdate(this, conn, pstm, bsql, domainObject) ;
					}else if(operation == 3){
						pls[i].preDelete(this, conn, pstm, bsql, domainObject) ;
					}else{
						throw new DaoException("unknown operation code:" + operation) ;
					}
				}
			}
			
			int affectedRows = pstm.executeUpdate() ;
			
			if(this.debugService.isLogSQL()){
				long timeCost = 0 ;
				if(measureTime){
					timeCost = System.nanoTime() - startTime ;
				}
				
				this.debugService.logSQL(bsql, timeCost) ;
			}
			
			return affectedRows ;
		}catch(SQLException e){
			throw new JDBCException("Error Code:" + e.getErrorCode() + ", sql:" + rawSQL, e, e.getSQLState()) ;
		}finally{
			CloseUtil.close(pstm) ;
		}
	}
	
	public int executeUpdate(BindedCompiledSQL bsql){
		ObjectMapping m = bsql.getCompiledSQLToRun().getMapping() ;
		String rawSQL = bsql.getSQLToRun() ;
		
		if(m == null){
			throw new DaoException("ObjectMapping is null. sql is:" + rawSQL) ;
		}
		
		DBGroup db = m.getDbGroup() ;
		PreparedStatement pstm = null;
		
		boolean measureTime = this.debugService.isMeasureTime() ;
		long startTime = 0L ;
		if(measureTime){
			startTime = System.nanoTime() ;
		}
		
		try {
			Connection conn = getConnection(db, bsql.getTableCondition()) ;
			pstm = conn.prepareStatement(rawSQL);
			this.applyQueryTimeout(pstm) ;
			bsql.prepareNamedParams(db.getDialect(), pstm) ;
			
			int affectedRows = pstm.executeUpdate() ;
			
			if(this.debugService.isLogSQL()){
				long timeCost = 0 ;
				if(measureTime){
					timeCost = System.nanoTime() - startTime ;
				}
				
				this.debugService.logSQL(bsql, timeCost) ;
			}
			
			return affectedRows ;
		}catch(SQLException e){
			throw new JDBCException("Error Code:" + e.getErrorCode() + ", sql:" + rawSQL, e, e.getSQLState()) ;
		}finally{
			CloseUtil.close(pstm) ;
		}
	}

	public Object loadPropForUpdate(Object domainObject, String propName) {
		String domainClassName = getRealDomainClass(domainObject).getName() ;
		
		POJOBasedObjectMapping mapping = (POJOBasedObjectMapping) this.omm.getObjectMapping(domainClassName, Guzz.getTableCondition()) ;
		if(mapping == null){
			throw new DaoException("ObjectMapping is null. class is:" + domainClassName) ;
		}
		
		Table table = mapping.getTable() ;
		TableColumn column = table.getColumnByPropName(propName) ;
		
		if(column == null){
			throw new DaoException("property [" + propName + "] not found in the mapped table of class: " + domainClassName) ;
		}
	
		//load the value from db
		if(column.getDataLoader() != null){
			return column.getDataLoader().loadLazyDataForWrite(this, domainObject) ;
		}else{
			CompiledSQL sqlForLoadLazy = this.compiledSQLManager.buildLoadColumnByPkSQL(mapping, column.getColNameForSQL()) ;
			Object pkValue = mapping.getBeanWrapper().getValue(domainObject, table.getPKPropName()) ;
			
			// 从主数据库读取，不进行缓存。
			return this.findCell00(sqlForLoadLazy.bind("guzz_pk", pkValue).setExceptionOnNoRecordFound(true).setLockMode(LockMode.UPGRADE), column.getSqlDataType()) ;
		}
		
		
//		要读取的属性并不一定都是lazy的，如：可能是非lazy的clob，这时下面的方法将出现错误。
//		BusinessDescriptor bd = mapping.getBusinessDescriptor() ;
//		
//		//continue here
//		LazyColumn lc = bd.match(propName) ;
//		if(lc == null){
//			throw new DaoException("property [" + propName + "] should be a lazy property or a custom loaded property.") ;
//		}
//		
//		return lc.getPropertyForWrite(this, domainObject) ;
	}
	
	public SQLBatcher createCompiledSQLBatcher(CompiledSQL sql) {
		return createCompiledSQLBatcher(sql, Guzz.getTableCondition()) ;
	}
	
	public SQLBatcher createCompiledSQLBatcher(CompiledSQL cs, Object tableCondition) {
		BindedCompiledSQL bsql = cs.bindNoParams().setTableCondition(tableCondition) ;
		String rawSQL = bsql.getSQLToRun() ;
		ObjectMapping m = bsql.getCompiledSQLToRun().getMapping() ;
		
		if(m == null){
			throw new ORMException("ObjectMapping not found. sql is:" + rawSQL) ;
		}
		
		DBGroup db = m.getDbGroup() ;
		PreparedStatement pstm = null;
		
		try {
			Connection conn = getConnection(db, bsql.getTableCondition()) ;
			pstm = conn.prepareStatement(rawSQL);
			this.applyQueryTimeout(pstm) ;
		}catch(SQLException e){
			CloseUtil.close(pstm) ;
			throw new JDBCException("Error Code:" + e.getErrorCode() + ", sql:" + rawSQL, e, e.getSQLState()) ;
		}
		
		if(this.psForBatch == null){
			this.psForBatch = new LinkedList() ;
		}
		
		this.psForBatch.add(pstm) ;
		
		SQLBatcherImpl b = new SQLBatcherImpl(this.debugService, pstm, rawSQL, db.getDialect(), bsql.getCompiledSQLToRun()) ;
		
		return b ;
	}
	
	public ObjectBatcher createObjectBatcher() {
		ObjectBatcherImpl b = new ObjectBatcherImpl(compiledSQLManager, this, this.debugService) ;
		
		if(this.objectBatchers == null){
			this.objectBatchers = new LinkedList() ;
		}
		
		this.objectBatchers.add(b) ;
		
		return b ;
	}
	
	public void commit(){
		this.connectionsGroup.commit() ;
	}
	
	public void rollback() throws DaoException {
		this.connectionsGroup.rollback() ;
	}

	public void close() {		
		if(this.psForBatch != null){
			Iterator i = this.psForBatch.iterator() ;
			while(i.hasNext()){
				PreparedStatement ps = (PreparedStatement) i.next() ;
				CloseUtil.close(ps) ;
			}
		}
		
		if(this.objectBatchers != null){
			Iterator i = objectBatchers.iterator() ;
			while(i.hasNext()){
				ObjectBatcherImpl b = (ObjectBatcherImpl) i.next() ;
				CloseUtil.close(b.getPreparedStatement()) ;
			}
		}
		
		//close connections.
		super.close();
	}
	
	public ReadonlyTranSession exportReadAPI() {
		ReadonlyTranSession r = exportNativeReadAPI() ;
		
		return (ReadonlyTranSession) Proxy.newProxyInstance(
				r.getClass().getClassLoader(), new Class[]{ReadonlyTranSession.class},
				new TranSessionCloseSuppressingInvocationHandler(r));
	}
	
	public ReadonlyTranSession exportNativeReadAPI() {
		if(read == null){
			read = new ReadonlyTranSessionImpl(this) ;
		}
		
		return read ;
	}

}

/**
 * Invocation handler that suppresses close calls on Guzz ReadonlyTranSession.
 */
class TranSessionCloseSuppressingInvocationHandler implements InvocationHandler {

	private final TranSession target;

	public TranSessionCloseSuppressingInvocationHandler(TranSession target) {
		this.target = target;
	}

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		// Invocation on Session interface coming in...

		if (method.getName().equals("equals")) {
			// Only consider equal when proxies are identical.
			return (proxy == args[0]);
		}else if (method.getName().equals("hashCode")) {
			// Use hashCode of Session proxy.
			return System.identityHashCode(proxy);
		}else if (method.getName().equals("close")) {
			// Handle close method: suppress, not valid.
			return null;
		}

		// Invoke method on target Session.
		try {
			Object retVal = method.invoke(this.target, args);
			
			return retVal;
		}
		catch (InvocationTargetException ex) {
			throw ex.getTargetException();
		}
	}
}


class WriteConnectionFetcher implements ConnectionFetcher{
	
	private boolean autoCommit ;
	
	public WriteConnectionFetcher(boolean autoCommit){
		this.autoCommit = autoCommit ;
	}
	
	public Connection getConnection(PhysicsDBGroup dbGroup) {
		return openRWConn(dbGroup) ;
	}
	
	public Connection openRWConn(PhysicsDBGroup dbGroup) {
		DatabaseService masterDatabaseService = dbGroup.getMasterDB() ;
		
		if(masterDatabaseService != null && masterDatabaseService.isAvailable()){
			Connection conn = null ;
			try {
				conn = masterDatabaseService.getDataSource().getConnection();				
			} catch (SQLException e) {
				//be careful of conn leak.
				CloseUtil.close(conn) ;
				
				throw new DaoException("master datasource failed.", e) ;
			}
			
			try {
				conn.setAutoCommit(autoCommit) ;
				
				return conn ;
			} catch (Exception e) {
				//be careful of conn leak.
				CloseUtil.close(conn) ;
				
				throw new DaoException("fail to set autoCommit to:[" + autoCommit + "]", e) ;
			}
		}else{
			throw new GuzzException("master database is not available.") ;
		}
	}
	
	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean allowDelay) {
		this.autoCommit = allowDelay;
	}
	
}
