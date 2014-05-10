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

import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.guzz.Guzz;
import org.guzz.connection.PhysicsDBGroup;
import org.guzz.jdbc.ObjectBatcher;
import org.guzz.jdbc.SQLBatcher;
import org.guzz.orm.se.SearchExpression;
import org.guzz.orm.se.Terms;
import org.guzz.orm.sql.CompiledSQL;
import org.guzz.test.Comment;
import org.guzz.test.DBBasedTestCase;
import org.guzz.test.User;
import org.guzz.transaction.ReadonlyTranSession;
import org.guzz.transaction.WriteTranSession;
import org.guzz.util.StringUtil;

/**
 * 
 * 
 *
 * @author liu kaixuan(liukaixuan@gmail.com)
 */
public class TestShadowTableView extends DBBasedTestCase {
	protected Connection H2CrossStitchConn = null ;
	
	protected void prepareEnv() throws Exception {
		super.prepareEnv();
		
		this.H2CrossStitchConn = ((PhysicsDBGroup) this.gf.getDBGroup("cargoDB.cargo2")).getMasterDB().getDataSource().getConnection() ;
		
		executeUpdate(H2CrossStitchConn, "drop table if exists TB_COMMENT2") ;
		String sql = "create table TB_COMMENT(id int not null primary key ,userId int(11), userName varchar(64), DESCRIPTION text, createdTime TIMESTAMP)" ;
    	executeUpdateNoException(H2CrossStitchConn, StringUtil.replaceString(sql, "TB_COMMENT", "TB_COMMENT2")) ;
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		
		this.H2CrossStitchConn.close() ;
	}
	
	public void testInsert() throws Exception{
		WriteTranSession session = tm.openRWTran(true) ;
		
		for(int i = 1 ; i < 1001 ; i++){
			Comment c = new Comment() ;
			
			c.setContent("my content") ;
			Date now = new Date() ;
			c.setCreatedTime(now) ;
			if(i % 2 == 0){
				c.setUserName("lucy") ;
			}else{
				c.setUserName("lily") ;
			}
			c.setUserId(i) ;
			
			User u = new User() ;
			u.setId(i) ;
			Guzz.setTableCondition(u) ;
			
			Integer pk = (Integer) session.insert(c) ;
			
			assertEquals(pk.intValue(), 100 * (20 + 1) + i - 1) ;
		}
		
		session.close() ;
	}
	
	public void testFindBySE() throws Exception{
		testInsert() ;
		
		ReadonlyTranSession session = tm.openDelayReadTran() ;
		
		User u = new User() ;
		u.setId(1) ;
		Guzz.setTableCondition(u) ;
		
		SearchExpression se = SearchExpression.forClass(Comment.class) ;
		se.and(Terms.bigger("userId", 0)) ;
		assertEquals(session.count(se), 500) ;
		
		//lily 500, lucy 0
		se.and(Terms.eq("userName", "lily")) ;
		assertEquals(session.count(se), 500) ;
		
		se = SearchExpression.forClass(Comment.class) ;
		se.and(Terms.eq("userName", "lucy")) ;
		assertEquals(session.count(se), 0) ;
		
		//SearchExpression.setTableCondition条件优先级高于Guzz.setTableCondition。此时应该只有lily有数据。
		se = SearchExpression.forClass(Comment.class) ;
		User u2 = new User() ;
		u2.setId(2) ;
		
		//lily 0, lucy 500
		se.and(Terms.eq("userName", "lily")).setTableCondition(u2) ;
		assertEquals(session.count(se), 0) ;
		
		se = SearchExpression.forClass(Comment.class) ;
		se.and(Terms.eq("userName", "lucy")).setTableCondition(u2) ;
		assertEquals(session.count(se), 500) ;
		
		session.close() ;
	}
	
	public void testCompiledSQL() throws Exception{
		testInsert() ;
		
		ReadonlyTranSession session = tm.openDelayReadTran() ;
		
		String sql = "select * from @@comment where userName = :userName" ;
		CompiledSQL cs = tm.getCompiledSQLBuilder().buildCompiledSQL(Comment.class, sql) ;
		cs.addParamPropMapping("userName", "userName") ;
		
		User u = new User() ;
		u.setId(1) ;

		User u2 = new User() ;
		u2.setId(2) ;
		
		Guzz.setTableCondition(u) ;
		assertEquals(session.list(cs.bind("userName", "lily"), 1, 1000).size(), 500) ;
		assertEquals(session.list(cs.bind("userName", "lucy"), 1, 1000).size(), 0) ;
		//bsql.setTableCondition条件优先级高于Guzz.setTableCondition。
		assertEquals(session.list(cs.bind("userName", "lily").setTableCondition(u2), 1, 1000).size(), 0) ;
		assertEquals(session.list(cs.bind("userName", "lucy").setTableCondition(u2), 1, 1000).size(), 500) ;
		
		Guzz.setTableCondition(u2) ;
		assertEquals(session.list(cs.bind("userName", "lily"), 1, 1000).size(), 0) ;
		assertEquals(session.list(cs.bind("userName", "lucy"), 1, 1000).size(), 500) ;
		//bsql.setTableCondition条件优先级高于Guzz.setTableCondition。
		assertEquals(session.list(cs.bind("userName", "lily").setTableCondition(u2), 1, 1000).size(), 0) ;
		assertEquals(session.list(cs.bind("userName", "lucy").setTableCondition(u2), 1, 1000).size(), 500) ;
		assertEquals(session.list(cs.bind("userName", "lily").setTableCondition(u), 1, 1000).size(), 500) ;
		assertEquals(session.list(cs.bind("userName", "lucy").setTableCondition(u), 1, 1000).size(), 0) ;
		
		session.close() ;
	}
	
	public void testObjectBatcher() throws Exception{
		WriteTranSession session = tm.openRWTran(false) ;
		ObjectBatcher batcher = session.createObjectBatcher() ;
		
		User u = new User() ;
		u.setId(1) ;
		Guzz.setTableCondition(u) ;
				
		for(int i = 1 ; i < 1001 ; i++){
			Comment c = new Comment() ;
			
			c.setContent("my content") ;
			Date now = new Date() ;
			c.setCreatedTime(now) ;
			if(i % 2 == 0){
				c.setUserName("lucy") ;
			}else{
				c.setUserName("lily") ;
			}
			c.setUserId(i) ;
			
			batcher.insert(c) ;
		}
		
		batcher.executeBatch() ;
		session.commit() ;
		
		session.close() ;
		
		ReadonlyTranSession read = tm.openNoDelayReadonlyTran() ;
		
		SearchExpression se = SearchExpression.forClass(Comment.class) ;
		assertEquals(read.count(se), 1000) ;
		assertEquals(read.count(se.setTableCondition(new User(2))), 0) ;
		
		read.close() ;
	}
	
	public void testSQLBatcher() throws Exception{
		WriteTranSession session = tm.openRWTran(false) ;

		String sql = "insert into @@comment(@userId, @userName, @content, @createdTime) values(:userId, :userName, :content, :createdTime)" ;
		CompiledSQL cs = tm.getCompiledSQLBuilder().buildCompiledSQL(Comment.class, sql) ;
		
		SQLBatcher batcher = session.createCompiledSQLBatcher(cs, new User(2)) ;
				
		User u = new User() ;
		u.setId(1) ;
		Guzz.setTableCondition(u) ;
				
		for(int i = 1 ; i < 1001 ; i++){
			HashMap params = new HashMap() ;
			params.put("userId", new Integer(i)) ;
			if(i % 2 == 0){
				params.put("userName", "lucy") ;
			}else{
				params.put("userName", "lily") ;
			}

			Date now = new Date() ;
			params.put("createdTime", now) ;
			
			params.put("content", "my content") ;
			
			batcher.addNewBatchParams(params) ;
		}
		
		batcher.executeBatch() ;
		session.commit() ;
		
		session.close() ;
		
		ReadonlyTranSession read = tm.openNoDelayReadonlyTran() ;
		
		SearchExpression se = SearchExpression.forClass(Comment.class) ;
		assertEquals(read.count(se), 0) ;
		assertEquals(read.count(se.setTableCondition(new User(2))), 1000) ;
		assertEquals(read.count(se.and(Terms.eq("userName", "lucy"))), 500) ;
		
		read.close() ;
	}
	
	public void testFindByIBatisId() throws Exception{
		testInsert() ;
		
		ReadonlyTranSession session = tm.openDelayReadTran() ;
		
		User u = new User() ;
		u.setId(1) ;
		Guzz.setTableCondition(u) ;
		
		HashMap params = new HashMap() ;
		params.put("userName", "lily") ;
		
		List comments = session.list("listCommentsByName", params, 1, 10000) ;
		assertEquals(comments.size(), 500) ;
		//ibatis模式的映射，内容应该没有映射。
		for(int i = 0 ; i < comments.size() ; i++){
			Comment c = (Comment) comments.get(i) ; 
			assertEquals(c.getContent(), null) ;
		}
		
		//lucy没有记录
		params.put("userName", "lucy") ;
		assertEquals(session.list("listCommentsByName", params, 1, 10000).size(), 0) ;
		
		//倒过来。
		Guzz.setTableCondition(new User(2)) ;
		params.put("userName", "lucy") ;
		assertEquals(session.list("listCommentsByName", params, 1, 10000).size(), 500) ;
		params.put("userName", "lily") ;
		assertEquals(session.list("listCommentsByName", params, 1, 10000).size(), 0) ;
		
		session.close() ;
	}
	
}
