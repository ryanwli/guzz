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
package org.guzz.orm.sql.impl;

import java.util.HashMap;
import java.util.Map;

import org.guzz.exception.GuzzException;
import org.guzz.orm.mapping.POJOBasedObjectMapping;
import org.guzz.orm.rdms.Table;
import org.guzz.orm.rdms.TableColumn;
import org.guzz.orm.sql.CompiledSQL;
import org.guzz.orm.sql.CompiledSQLBuilder;
import org.guzz.orm.sql.CompiledSQLManager;
import org.guzz.orm.sql.CustomCompiledSQL;
import org.guzz.orm.sql.MarkedSQL;
import org.guzz.orm.sql.NormalCompiledSQL;
import org.guzz.orm.sql.CustomCompiledSQL.DynamicSQLProvider;
import org.guzz.service.core.DynamicSQLService;
import org.guzz.util.StringUtil;

/**
 * 
 * 
 *
 * @author liukaixuan(liukaixuan@gmail.com)
 */
public class CompiledSQLManagerImpl implements CompiledSQLManager {
	
	private Map sqls = new HashMap() ;
	
	private final CompiledSQLBuilder compiledSQLBuilder ;
	
	private DynamicSQLService dynamicSQLService ;
	
	public CompiledSQLManagerImpl(CompiledSQLBuilder compiledSQLBuilder){
		this.compiledSQLBuilder = compiledSQLBuilder ;
	}

	public CompiledSQL getSQL(String id) {
		if(dynamicSQLService == null){
			return (CompiledSQL) sqls.get(id) ;
		}
		
		if(dynamicSQLService.overrideSqlInGuzzXML()){
			CompiledSQL cs = this.dynamicSQLService.getSql(id) ;
			
			if(cs == null){
				cs = (CompiledSQL) sqls.get(id) ;
			}
			
			return cs ;
		}else{
			CompiledSQL cs = (CompiledSQL) sqls.get(id) ;
			
			if(cs == null){
				cs = this.dynamicSQLService.getSql(id) ;
			}
			
			return cs ;
		}
	}
	
	public void addCompliedSQL(String id, CompiledSQL cs){
		sqls.put(id, cs) ;
	}	
		
	public void addDomainBusiness(POJOBasedObjectMapping mapping){
		Table table = mapping.getTable() ;
		String[] names = mapping.getUniqueName() ;
		
		//注册compiledSQL
		for(int i = 0 ; i < names.length ; i++){
			String businessName = names[i] ;
			
			if(mapping.getTable().getIdentifierGenerator().insertWithPKColumn()){
				if(table.isCustomTable()){
					this.addCompliedSQL(CS_PREFIX.BUSINESS_INSERT_PREFIX + businessName, buildCustomInsertSQLWithPK(mapping)) ;
				}else{
					this.addCompliedSQL(CS_PREFIX.BUSINESS_INSERT_PREFIX + businessName, buildNormalInsertSQLWithPK(mapping)) ;
				}
			}else{
				if(table.isCustomTable()){
					this.addCompliedSQL(CS_PREFIX.BUSINESS_INSERT_PREFIX + businessName, buildCustomInsertSQLWithoutPK(mapping)) ;
				}else{
					this.addCompliedSQL(CS_PREFIX.BUSINESS_INSERT_PREFIX + businessName, buildNormalInsertSQLWithoutPK(mapping)) ;
				}
			}
			
			if(table.isCustomTable()){
				this.addCompliedSQL(CS_PREFIX.BUSINESS_UPDATE_PREFIX + businessName, buildCustomUpdateSQL(mapping)) ;
			}else{
				this.addCompliedSQL(CS_PREFIX.BUSINESS_UPDATE_PREFIX + businessName, buildNormalUpdateSQL(mapping)) ;
			}
			
			if(table.isCustomTable()){
				this.addCompliedSQL(CS_PREFIX.BUSINESS_DELETE_PREFIX + businessName, buildCustomDeleteSQL(mapping)) ;
			}else{
				this.addCompliedSQL(CS_PREFIX.BUSINESS_DELETE_PREFIX + businessName, buildNormalDeleteSQL(mapping)) ;
			}
			
			if(table.isCustomTable()){
				this.addCompliedSQL(CS_PREFIX.BUSINESS_SELECT_PREFIX + businessName, buildCustomSelectSQL(mapping)) ;
			}else{
				this.addCompliedSQL(CS_PREFIX.BUSINESS_SELECT_PREFIX + businessName, buildNormalSelectSQL(mapping)) ;
			}
		}
		
		//注册
	}
	
	public CompiledSQL getDefinedSelectSQL(String className){
		String key = CompiledSQLManager.CS_PREFIX.BUSINESS_SELECT_PREFIX + className ;
		return getSQL(key) ;
	}
	
	public CompiledSQL getDefinedInsertSQL(String className){
		String key = CompiledSQLManager.CS_PREFIX.BUSINESS_INSERT_PREFIX + className ;
		return getSQL(key) ;
	}
	
	public CompiledSQL getDefinedUpdateSQL(String className){
		String key = CompiledSQLManager.CS_PREFIX.BUSINESS_UPDATE_PREFIX + className ;
		return getSQL(key) ;
	}
	
	public CompiledSQL getDefinedDeleteSQL(String className){
		String key = CompiledSQLManager.CS_PREFIX.BUSINESS_DELETE_PREFIX + className ;
		return getSQL(key) ;
	}
	
	protected CustomCompiledSQL buildCustomInsertSQLWithoutPK(POJOBasedObjectMapping mapping){		
		return compiledSQLBuilder.buildCustomCompiledSQL(mapping.getBusiness().getName(), 
			new DynamicSQLProvider(){
				public NormalCompiledSQL getSql(POJOBasedObjectMapping m) {
					return buildNormalInsertSQLWithoutPK(m) ;
				}
			}
		) ;
	}
	
	protected NormalCompiledSQL buildNormalInsertSQLWithoutPK(POJOBasedObjectMapping mapping){
		Table table = mapping.getTable() ;
		
		TableColumn[] columns = table.getColumnsForInsert() ;
//		String primaryProp = mapping.getTable().getPKPropName() ;
		String primaryColumn = table.getPKColumn().getColNameForSQL() ;
		
		HashMap paramPropMapping = new HashMap() ;
		
		//insert
		StringBuffer sb_insert = new StringBuffer() ;
		
		sb_insert.append("insert into ")
				 .append(MarkedSQL.TABLE_START_TAG_IN_MARKED_SQL)
				 .append(mapping.getBusiness().getName())
				 .append("(") ;
		
		boolean firstProp = true ;
		
		for(int i = 0 ; i < columns.length ; i++){
			if(primaryColumn.equals(columns[i].getColNameForSQL())) continue ;
						
			if(!firstProp){
				sb_insert.append(", ") ;
			}else{
				firstProp = false ;
			}
			
			sb_insert.append(columns[i].getColNameForSQL()) ;
		}		
		
		sb_insert.append(") values(") ;
		
		firstProp = true ;
		for(int i = 0 ; i < columns.length ; i++){
			if(primaryColumn.equals(columns[i].getColNameForSQL())) continue ;
			
			if(!firstProp){
				sb_insert.append(", ") ;
			}else{
				firstProp = false ;
			}
			
			String propName = columns[i].getPropName() ;
			sb_insert.append(":").append(propName) ;
			
			paramPropMapping.put(propName, propName) ;
		}
		
		sb_insert.append(")") ;
		
		NormalCompiledSQL cs = compiledSQLBuilder.buildCompiledSQL(mapping, sb_insert.toString()) ;
		cs.setParamPropMapping(paramPropMapping) ;
		
		return cs ;
	}
	
	protected CustomCompiledSQL buildCustomInsertSQLWithPK(POJOBasedObjectMapping mapping){		
		return compiledSQLBuilder.buildCustomCompiledSQL(mapping.getBusiness().getName(), 
			new DynamicSQLProvider(){
				public NormalCompiledSQL getSql(POJOBasedObjectMapping m) {
					return buildNormalInsertSQLWithPK(m) ;
				}
			}
		) ;
	}
	
	protected NormalCompiledSQL buildNormalInsertSQLWithPK(POJOBasedObjectMapping mapping){
		TableColumn[] columns = mapping.getTable().getColumnsForInsert() ;
		
		//insert
		StringBuffer sb_insert = new StringBuffer() ;
		HashMap paramPropMapping = new HashMap() ;
		
		sb_insert.append("insert into ")
				 .append(MarkedSQL.TABLE_START_TAG_IN_MARKED_SQL)
				 .append(mapping.getBusiness().getName())
				 .append("(") ;
		
		for(int i = 0 ; i < columns.length ; i++){
			if(i > 0){
				sb_insert.append(", ") ;
			}
			sb_insert.append(columns[i].getColNameForSQL()) ;
		}
		
		
		sb_insert.append(") values(") ;
		
		for(int i = 0 ; i < columns.length ; i++){
			if(i > 0){
				sb_insert.append(", ") ;
			}
			String propName = columns[i].getPropName() ;
			
			sb_insert.append(":").append(propName) ;
			paramPropMapping.put(propName, propName) ;
		}
		
		sb_insert.append(")") ;
		
		NormalCompiledSQL cs = (NormalCompiledSQL) compiledSQLBuilder.buildCompiledSQL(mapping, sb_insert.toString()).setParamPropMapping(paramPropMapping) ;
		
		return cs ;
	}
	
	protected CustomCompiledSQL buildCustomUpdateSQL(POJOBasedObjectMapping mapping){		
		return compiledSQLBuilder.buildCustomCompiledSQL(mapping.getBusiness().getName(), 
			new DynamicSQLProvider(){
				public NormalCompiledSQL getSql(POJOBasedObjectMapping m) {
					return buildNormalUpdateSQL(m) ;
				}
			}
		) ;
	}
	
	//update by primary key.
	protected NormalCompiledSQL buildNormalUpdateSQL(POJOBasedObjectMapping mapping){
		Table table = mapping.getTable() ;
		
		TableColumn[] columns = table.getColumnsForUpdate() ;
		String primaryKey = table.getPKColumn().getColNameForSQL() ;
		String primaryProp = table.getPKPropName() ;
		if(StringUtil.isEmpty(primaryProp)){
			throw new GuzzException("business domain must have a primary key. table:" + table.getConfigTableName()) ;
		}
		
		String versionColName = table.getVersionColumn() == null ? null : table.getVersionColumn().getColNameForSQL() ;
		String versionProp = table.getVersionColumn() == null ? null : table.getVersionColumn().getPropName();

		StringBuffer sb = new StringBuffer() ;
		HashMap paramPropMapping = new HashMap() ;
		
		sb.append("update ")
		  .append(MarkedSQL.TABLE_START_TAG_IN_MARKED_SQL)
		  .append(mapping.getBusiness().getName())
//		  .append(mapping.getTable().getTableName())
		  .append(" set ") ;
		
		boolean firstProp = true ;
		for(int i = 0 ; i < columns.length ; i++){
			if(primaryKey.equals(columns[i].getColNameForSQL())) continue ;
			
			if(!firstProp){
				sb.append(", ") ;
			}else{
				firstProp = false ;
			}
			
			String propName = columns[i].getPropName() ;
			if (versionColName != null && propName.equals(versionProp)) {
				sb.append(versionColName + " = " + versionColName + " + 1");
			} else {
				sb.append(columns[i].getColNameForSQL()).append("=:").append(propName) ;
				paramPropMapping.put(propName, propName) ;
			}
		}
		
		sb.append(" where ")
		  .append(primaryKey)
		  .append("=:")
		  .append(primaryProp) ;
		
		if(versionColName != null) {
			sb.append(" and ").append(versionColName)
			  .append("=:")
			  .append(versionProp) ;
			
			paramPropMapping.put(versionProp, versionProp) ;			
		}
		
		NormalCompiledSQL cs = (NormalCompiledSQL) compiledSQLBuilder.buildCompiledSQL(mapping, sb.toString()).setParamPropMapping(paramPropMapping) ;
		cs.addParamPropMapping(primaryProp, primaryProp) ;
		
		return cs ;
	}
	
	protected CustomCompiledSQL buildCustomDeleteSQL(POJOBasedObjectMapping mapping){		
		return compiledSQLBuilder.buildCustomCompiledSQL(mapping.getBusiness().getName(), 
			new DynamicSQLProvider(){
				public NormalCompiledSQL getSql(POJOBasedObjectMapping m) {
					return buildNormalDeleteSQL(m) ;
				}
			}
		) ;
	}
	
	//delete one object by primary key.
	protected NormalCompiledSQL buildNormalDeleteSQL(POJOBasedObjectMapping mapping){
		Table table = mapping.getTable() ;
		
		String primaryKey = table.getPKColumn().getColNameForSQL() ;
		String primaryProp = table.getPKPropName() ;
		if(StringUtil.isEmpty(primaryProp)){
			throw new GuzzException("business domain must has a primary key. table:" + table.getConfigTableName()) ;
		}
		
		StringBuffer sb = new StringBuffer() ;
		
		sb.append("delete from ")
		  .append(MarkedSQL.TABLE_START_TAG_IN_MARKED_SQL)
		  .append(mapping.getBusiness().getName())
//		  .append(mapping.getTable().getTableName())
		  .append(" where ")		  
		  .append(primaryKey)
		  .append("=:")
		  .append(primaryProp);		
		
		NormalCompiledSQL cs = compiledSQLBuilder.buildCompiledSQL(mapping, sb.toString()) ;
		cs.addParamPropMapping(primaryProp, primaryProp) ;
		
		return cs ;
	}
	
	protected CustomCompiledSQL buildCustomSelectSQL(POJOBasedObjectMapping mapping){		
		return compiledSQLBuilder.buildCustomCompiledSQL(mapping.getBusiness().getName(), 
			new DynamicSQLProvider(){
				public NormalCompiledSQL getSql(POJOBasedObjectMapping m) {
					return buildNormalSelectSQL(m) ;
				}
			}
		) ;
	}
	
	//select one object by primary key.
	protected NormalCompiledSQL buildNormalSelectSQL(POJOBasedObjectMapping mapping){
		Table table = mapping.getTable() ;
		
		String primaryKey = table.getPKColumn().getColNameForSQL() ;
		String primaryProp = table.getPKPropName() ;
		TableColumn[] columns = table.getColumnsForSelect() ;
		
		if(StringUtil.isEmpty(primaryProp)){
			throw new GuzzException("business domain must has a primary key. table:" + table.getConfigTableName()) ;
		}
		
		StringBuffer sb = new StringBuffer() ;
		sb.append("select ") ;
		
		boolean firstProp = true ;
		for(int i = 0 ; i < columns.length ; i++){
			if(!firstProp){
				sb.append(", ") ;
			}else{
				firstProp = false ;
			}
			
			sb.append(columns[i].getColNameForSQL()) ;
		}
		
		sb.append(" from ")
		  .append(MarkedSQL.TABLE_START_TAG_IN_MARKED_SQL)
		  .append(mapping.getBusiness().getName())
//		  .append(mapping.getTable().getTableName())
		  .append(" where ")		  
		  .append(primaryKey)
		  .append("=:")
		  .append(primaryProp);
		
		NormalCompiledSQL cs = compiledSQLBuilder.buildCompiledSQL(mapping, sb.toString()) ;
		cs.addParamPropMapping(primaryProp, primaryProp) ;
		
		return cs ;
	}
	
	//update by primary key.
	public CompiledSQL buildUpdateSQL(POJOBasedObjectMapping mapping, String[] propsToUpdate){
		Table table = mapping.getTable() ;
		
		String primaryKey = table.getPKColumn().getColNameForSQL() ;
		String primaryProp = table.getPKPropName() ;
		if(StringUtil.isEmpty(primaryProp)){
			throw new GuzzException("business domain must has a primary key. table:" + table.getConfigTableName()) ;
		}
		
		StringBuffer sb = new StringBuffer() ;
		HashMap paramPropMapping = new HashMap() ;
		
		sb.append("update ")
		  .append(MarkedSQL.TABLE_START_TAG_IN_MARKED_SQL)
		  .append(mapping.getBusiness().getName())
//		  .append(mapping.getTable().getTableName())
		  .append(" set ") ;
		
		boolean firstProp = true ;
		for(int i = 0 ; i < propsToUpdate.length ; i++){
			String propName = propsToUpdate[i] ;
			
			if(primaryProp.equals(propName)) continue ;
			
			if(!firstProp){
				sb.append(", ") ;
			}else{
				firstProp = false ;
			}			
			
			sb.append("@").append(propName).append("=:").append(propName) ;
			paramPropMapping.put(propName, propName) ;
		}
		
		sb.append(" where ")
		  .append(primaryKey)
		  .append("=:")
		  .append(primaryProp) ;		
		
		CompiledSQL cs = compiledSQLBuilder.buildCompiledSQL(mapping, sb.toString()).setParamPropMapping(paramPropMapping) ;
		cs.addParamPropMapping(primaryProp, primaryProp) ;
		
		return cs ;
	}
	
	public CompiledSQL buildLoadColumnByPkSQL(POJOBasedObjectMapping mapping, String columnName){
		Table table = mapping.getTable() ;
		
		StringBuffer sb = new StringBuffer(64) ;
		
		sb.append("select ")
		  .append(columnName)
		  .append(" from ")
		  .append(MarkedSQL.TABLE_START_TAG_IN_MARKED_SQL)
		  .append(mapping.getBusiness().getName())
//		  .append(table.getTableName())
		  .append(" where ")
		  .append(table.getPKColumn().getColNameForSQL())
		  .append("=:guzz_pk");
		
		CompiledSQL sqlForLoadProp = compiledSQLBuilder.buildCompiledSQL(mapping, sb.toString()) ;
		sqlForLoadProp.addParamPropMapping("guzz_pk", table.getPKPropName()) ;
		
		return sqlForLoadProp ;
	}

	public CompiledSQLBuilder getCompiledSQLBuilder() {
		return compiledSQLBuilder;
	}

	public DynamicSQLService getDynamicSQLService() {
		return dynamicSQLService;
	}

	public void setDynamicSQLService(DynamicSQLService dynamicSQLService) {
		this.dynamicSQLService = dynamicSQLService;
	}

}
