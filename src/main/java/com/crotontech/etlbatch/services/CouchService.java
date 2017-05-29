package com.crotontech.etlbatch.services;

import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.crotontech.etlbatch.repository.EtlRepository;

@Component
public class CouchService {
	@Autowired
	private EtlRepository etlRepository;

	public List<JSONObject> readEvents() throws JSONException {
		return etlRepository.findEventsByServerVersion(0, 1000);
	}

	public List<JSONObject> readClients() throws JSONException {
		return etlRepository.findClientsByServerVersion(0, 1000);
	}

}
