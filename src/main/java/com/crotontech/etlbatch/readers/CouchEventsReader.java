package com.crotontech.etlbatch.readers;

import java.util.List;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.crotontech.etlbatch.services.CouchService;

@Component
public class CouchEventsReader implements InitializingBean {

	private static final Logger log = LoggerFactory.getLogger(CouchEventsReader.class);
	@Autowired
	private CouchService couchService;
	private List<JSONObject> events;

	public void afterPropertiesSet() throws Exception {
		this.events = couchService.readEvents();

	}

	public JSONObject nextEvent() {
		if (events.size() > 0) {
			return events.remove(0);
		} else {
			log.info("Reader: no records found");
			return null;
		}
	}

}
