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
package org.guzz.test;

import javax.persistence.Column;
import javax.persistence.FetchType;

import org.guzz.pojo.lob.TranBlob;
import org.guzz.pojo.lob.TranClob;

/**
 * 
 * 
 *
 * @author liukaixuan(liukaixuan@gmail.com)
 */

@javax.persistence.Entity
@org.guzz.annotations.Entity(businessName="userInfo2")
@org.guzz.annotations.Table(name="TB_USER_INFO2")
public class UserInfoH2 {
	
	private int id ;
	
	private String userId ;
	
	private TranClob aboutMe ;
	
	private TranBlob portraitImg ;

	@javax.persistence.Id
	@Column(name="pk")
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@javax.persistence.Basic(fetch=FetchType.LAZY)
	public TranClob getAboutMe() {
		return aboutMe;
	}

	public void setAboutMe(TranClob aboutMe) {
		this.aboutMe = aboutMe;
	}

	@javax.persistence.Basic(fetch=FetchType.EAGER)
	public TranBlob getPortraitImg() {
		return portraitImg;
	}

	public void setPortraitImg(TranBlob portraitImg) {
		this.portraitImg = portraitImg;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

}
