package com.crotontech.etlbatch.writers;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.support.annotation.Classifier;
import org.springframework.stereotype.Component;

@Component
public class EventRouterClassifier {
	private static final Logger log = LoggerFactory.getLogger(EventRouterClassifier.class);

	@Classifier
	public String classify(Map<String,Object> classifiable) {
		
		return classifiable.get("").toString();
	}
}
