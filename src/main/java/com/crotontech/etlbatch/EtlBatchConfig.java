package com.crotontech.etlbatch;

import java.net.MalformedURLException;
import java.util.Date;

import javax.sql.DataSource;

import org.codehaus.jettison.json.JSONObject;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.crotontech.etlbatch.domain.CompositeItem;
import com.crotontech.etlbatch.listeners.JobCompletionNotificationListener;
import com.crotontech.etlbatch.processors.ClientProcessor;
import com.crotontech.etlbatch.processors.EventProcessor;
import com.crotontech.etlbatch.readers.CouchClientsReader;
import com.crotontech.etlbatch.readers.CouchEventsReader;
import com.crotontech.etlbatch.writers.EventRouterClassifier;
import com.crotontech.etlbatch.writers.JdbcClientItemWriter;

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
	CouchClientsReader couchClientsReader;

	@Autowired
	EventProcessor eventProcessor;

	@Autowired
	ClientProcessor clientProcessor;

	@Autowired
	JdbcClientItemWriter jdbcClientItemWriter;

	JobParameters parameters = new JobParametersBuilder().addDate("date", new Date()).toJobParameters();

	@Bean
	public Job importEventsJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("etlJob").incrementer(new RunIdIncrementer()).listener(listener).start(step1())
				.next(step2()).build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<JSONObject, CompositeItem>chunk(100).reader(clientJsonReader())
				.processor(clientProcessor).writer(jdbcClientItemWriter).taskExecutor(taskExecutor()).build();
	}

	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2").<JSONObject, CompositeItem>chunk(100).reader(eventJsonReader())
				.processor(eventProcessor).writer(eventItemWriter()).taskExecutor(taskExecutor()).build();
	}
	@Bean
	public TaskExecutor taskExecutor(){
	    SimpleAsyncTaskExecutor asyncTaskExecutor=new SimpleAsyncTaskExecutor(EtlBatchConfig.class.getCanonicalName());
	    int cores = Runtime.getRuntime().availableProcessors();
	    asyncTaskExecutor.setConcurrencyLimit(cores);
	    return asyncTaskExecutor;
	}
	@Bean
	public ItemReaderAdapter<JSONObject> eventJsonReader() {
		ItemReaderAdapter<JSONObject> readerAdapter = new ItemReaderAdapter<JSONObject>();
		readerAdapter.setTargetObject(couchEventsReader);
		readerAdapter.setTargetMethod("nextEvent");
		return readerAdapter;

	}

	@Bean
	public ItemReaderAdapter<JSONObject> clientJsonReader() {
		ItemReaderAdapter<JSONObject> readerAdapter = new ItemReaderAdapter<JSONObject>();
		readerAdapter.setTargetObject(couchClientsReader);
		readerAdapter.setTargetMethod("nextClient");
		return readerAdapter;

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

	// @Bean
	// public BackToBackPatternClassifier<CompositeItem, ItemWriter<? super
	// CompositeItem>> eventItemClassifier() {
	// BackToBackPatternClassifier<CompositeItem, ItemWriter<? super
	// CompositeItem>> classifier = new
	// BackToBackPatternClassifier<CompositeItem, ItemWriter<? super
	// CompositeItem>>();
	//
	// classifier.setRouterDelegate(eventRouterClassifier);
	//// Map<String, ItemWriter<? super CompositeItem>> map = new
	// HashMap<String, ItemWriter<? super CompositeItem>>();
	//// map.put("event", eventWriter());
	//// classifier.setMatcherMap(map);
	// return classifier;
	// }

	@Bean
	public ClassifierCompositeItemWriter<CompositeItem> eventItemWriter() {
		ClassifierCompositeItemWriter<CompositeItem> classifierCompositeItemWriter = new ClassifierCompositeItemWriter<CompositeItem>();
		classifierCompositeItemWriter.setClassifier(eventRouterClassifier);
		return classifierCompositeItemWriter;
	}

}
