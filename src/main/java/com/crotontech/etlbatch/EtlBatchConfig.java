package com.crotontech.etlbatch;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.codehaus.jettison.json.JSONObject;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.classify.BackToBackPatternClassifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.crotontech.etlbatch.domain.CompositeItem;
import com.crotontech.etlbatch.listeners.JobCompletionNotificationListener;
import com.crotontech.etlbatch.processors.EventProcessor;
import com.crotontech.etlbatch.readers.CouchEventsReader;
import com.crotontech.etlbatch.writers.EventRouterClassifier;

@Configuration
@EnableAutoConfiguration
@EnableBatchProcessing
@ComponentScan("com.crotontech")
public class EtlBatchConfig {
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	EventRouterClassifier eventRouterClassifier;

	@Autowired
	CouchEventsReader couchEventsReader;
	
	@Autowired
	EventProcessor eventProcessor;

	@Bean
	public Job importEventsJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("importEventsJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(step1()).end().build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<JSONObject, CompositeItem>chunk(100).reader(eventJsonReader())
				.processor(eventProcessor).writer(eventItemWriter()).build();
	}

	@Bean
	public ItemReaderAdapter<JSONObject> eventJsonReader() {
		ItemReaderAdapter<JSONObject> readerAdapter = new ItemReaderAdapter<JSONObject>();
		readerAdapter.setTargetObject(couchEventsReader);
		readerAdapter.setTargetMethod("nextEvent");
		return readerAdapter;

	}

	@Bean
	public ItemProcessor<JSONObject, CompositeItem> eventJsonProcessor() {
		return null;
	}

	@Bean
	public JdbcBatchItemWriter<CompositeItem> eventWriter() {

		return null;
	}

	@Bean
	public CouchDbConnector dbClient(@Value("${couchdb.name}") String dbName,

			@Value("${couchdb.protocol}") String dbProtocol,

			@Value("${couchdb.host}") String dbHost,

			@Value("${couchdb.port}") Integer dbPort,

			@Value("${couchdb.max.connections}") Integer dbMaxConnections,

			@Value("${couchdb.username}") String dbUserName,

			@Value("${couchdb.password}") String dbPassword) throws MalformedURLException {
		HttpClient authenticatedHttpClient = new StdHttpClient.Builder().url(dbProtocol + "://" + dbHost + ":" + dbPort)
				.username(dbUserName).password(dbPassword).build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(authenticatedHttpClient);
		// if the second parameter is true, the database will be created if it
		// doesn't exists
		CouchDbConnector db = dbInstance.createConnector(dbName, true);
		return db;
	}

	@Bean
	public BackToBackPatternClassifier<CompositeItem, ItemWriter<? super CompositeItem>> eventItemClassifier() {
		BackToBackPatternClassifier<CompositeItem, ItemWriter<? super CompositeItem>> classifier = new BackToBackPatternClassifier<CompositeItem, ItemWriter<? super CompositeItem>>();

		classifier.setRouterDelegate(eventRouterClassifier);
//		Map<String, ItemWriter<? super CompositeItem>> map = new HashMap<String, ItemWriter<? super CompositeItem>>();
//		map.put("event", eventWriter());
//		classifier.setMatcherMap(map);
		return classifier;
	}

	@Bean
	public ClassifierCompositeItemWriter<CompositeItem> eventItemWriter() {
		ClassifierCompositeItemWriter<CompositeItem> classifierCompositeItemWriter = new ClassifierCompositeItemWriter<CompositeItem>();
		classifierCompositeItemWriter.setClassifier(eventItemClassifier());
		return classifierCompositeItemWriter;
	}

	
}
