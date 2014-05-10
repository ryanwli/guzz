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
package org.guzz.orm.se;

import org.guzz.orm.ObjectMapping;
import org.guzz.util.StringUtil;

/**
 * 
 * 
 * @author liukaixuan(liukaixuan@gmail.com)
 */
public class OrTerm extends AbstractConcatTerm {
	
	public OrTerm() {
		
	}

	public OrTerm(SearchTerm leftTerm, SearchTerm rightTerm) {
		this.leftTerm = leftTerm ;
		this.rightTerm = rightTerm ;
	}
	
	public OrTerm or(SearchTerm term){
		if(term == null) return this;
		
		if(leftTerm == null && rightTerm == null){ //两边都为空
			leftTerm = term ;
		}else if(leftTerm == null){//左边为空，右边不为空；
			leftTerm = rightTerm ;
			rightTerm = term ;
		}else if(rightTerm == null){ //左边不为空; 右边为空 
			rightTerm = term ;
		}else{ //两边都不为空
			leftTerm = new OrTerm(leftTerm, rightTerm) ;
			rightTerm = term ;
		}
		return this ;
	}

	public String toExpression(SearchExpression se, ObjectMapping mapping, SearchParams params) {
		String left = leftTerm == null ? "" : leftTerm.toExpression(se, mapping, params) ;
		String right = rightTerm == null ? "" : rightTerm.toExpression(se, mapping, params) ;
		
		if(StringUtil.isEmpty(left)){
			return right ;
		}else if(StringUtil.isEmpty(right)){
			return left ;
		}else{
			StringBuilder sb = new StringBuilder() ;
			
			if(leftTerm instanceof AbstractConcatTerm){
				sb.append(" (").append(left).append(") or ") ;
			}else{
				sb.append(" ").append(left).append(" or ") ;
			}
			
			if(rightTerm instanceof AbstractConcatTerm){
				sb.append(" (").append(right).append(") ") ;
			}else{
				sb.append(" ").append(right).append(" ") ;
			}
			
			return sb.toString() ;
		}
	}

	public boolean isEmptyQuery() {
		if(this.leftTerm == null && this.rightTerm == null) return false ;
		if(this.leftTerm == null) return this.rightTerm.isEmptyQuery() ;
		if(this.rightTerm == null) return this.leftTerm.isEmptyQuery() ;
		
		return this.leftTerm.isEmptyQuery() && this.rightTerm.isEmptyQuery() ;
	}

}
