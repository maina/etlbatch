package com.crotontech.etlbatch;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;

public class EtlBatchMain implements CommandLineRunner {
	private static final Logger log = LoggerFactory.getLogger(EtlBatchMain.class);

	@Autowired
	@Qualifier("pwsDailyJob")
	private Job job;

	@Autowired
	private JobLauncher jobLauncher;

	JobParameters jobParameters;

	public static void main(String[] args) {

		SpringApplication.run(EtlBatchConfig.class, args);
	}

	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub

	}

	public JobExecution execute(String orderDate, String test) throws JobExecutionException, ParseException {
		if (orderDate == null || "".compareTo(orderDate.trim()) == 0) {
			throw new JobExecutionException("can not run the job as Date is missing or invalid");
		}

		JobParametersBuilder jobParametersbuilder = new JobParametersBuilder().addDate("orderDate", getDate(orderDate)); // dd-MM-yyyy
		if (!(test == null || "".compareTo(test.trim()) == 0))
			jobParametersbuilder.addString("test", test);
		jobParameters = jobParametersbuilder.toJobParameters();
		log.info("Launching Job '" + job.getName() + "' with Parameters: " + jobParameters);
		// Launch the job
		JobExecution jobExecution = jobLauncher.run(job, jobParameters);
		return jobExecution;
	}

	private Date getDate(String dateStr) throws ParseException {
		DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
		Date today = df.parse(dateStr);
		return today;
	}

}
