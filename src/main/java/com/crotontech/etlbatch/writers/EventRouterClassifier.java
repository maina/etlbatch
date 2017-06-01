package com.crotontech.etlbatch.writers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.annotation.Classifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.crotontech.etlbatch.domain.CompositeItem;

@Component
public class EventRouterClassifier {
	private static final Logger log = LoggerFactory.getLogger(EventRouterClassifier.class);
	@Autowired
	JdbcItemWriter itemWriter;

	@Classifier
	public ItemWriter<? super CompositeItem> classify(CompositeItem classifiable) {
		//classifiable.getEntityTypes().equals("");
		return itemWriter;
	}
}
