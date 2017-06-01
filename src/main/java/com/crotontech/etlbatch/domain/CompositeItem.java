package com.crotontech.etlbatch.domain;

import java.util.HashMap;
import java.util.Map;


public class CompositeItem {
	Map<String,String> entityTypes=new HashMap<String, String>();

	public Map<String, String> getEntityTypes() {
		return entityTypes;
	}

	public void setEntityTypes(Map<String, String> entityTypes) {
		this.entityTypes = entityTypes;
	}
	

}
