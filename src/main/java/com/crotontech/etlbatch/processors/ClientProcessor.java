package com.crotontech.etlbatch.processors;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.crotontech.etlbatch.domain.CompositeItem;

@Component
public class ClientProcessor implements ItemProcessor<JSONObject, CompositeItem> {
	private static final Logger log = LoggerFactory.getLogger(ClientProcessor.class);

	public CompositeItem process(JSONObject eventObject) throws Exception {
		CompositeItem item= new CompositeItem();
		return item;
	}

}
