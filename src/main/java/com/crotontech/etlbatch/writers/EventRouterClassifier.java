package com.crotontech.etlbatch.writers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.annotation.Classifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.crotontech.etlbatch.domain.CompositeItem;

@Component
public class EventRouterClassifier implements org.springframework.classify.Classifier<CompositeItem, ItemWriter<? super CompositeItem>> {
	
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(EventRouterClassifier.class);
	@Autowired
	JdbcEventItemWriter itemWriter;

	@Classifier
	public ItemWriter<? super CompositeItem> classify(CompositeItem classifiable) {
		//classifiable.getEntityTypes().equals("");
		return itemWriter;
	}
}
