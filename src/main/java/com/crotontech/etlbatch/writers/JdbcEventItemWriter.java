package com.crotontech.etlbatch.writers;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.crotontech.etlbatch.domain.CompositeItem;
@Component
public class JdbcEventItemWriter implements ItemWriter<CompositeItem>{
@Autowired
private JdbcTemplate jdbcTemplate;
	public void write(List<? extends CompositeItem> items) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
