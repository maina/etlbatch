package com.crotontech.etlbatch.repository;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.ektorp.ComplexKey;
import org.ektorp.CouchDbConnector;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.support.CouchDbRepositorySupport;
import org.ektorp.support.View;
import org.ektorp.support.Views;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Views({ @View(name = "events_by_server_version2", map = "function(doc) { if (doc.type === 'Event') { emit([doc.serverVersion], null); } }"),
		@View(name = "clients_by_server_version2", map = "function(doc) { if (doc.type === 'Client') { emit([doc.serverVersion], null); } }") })
@Component
public class EtlRepository extends CouchDbRepositorySupport<JSONObject> {
	private CouchDbConnector dbClient;

	protected EtlRepository(@Autowired CouchDbConnector db) {
		super(JSONObject.class, db);
		dbClient = db;
	}

	public List<JSONObject> findEventsByServerVersion(long serverVersion, int limit) throws JSONException {
		ComplexKey startKey = ComplexKey.of(serverVersion + 1);
		ComplexKey endKey = ComplexKey.of(Long.MAX_VALUE);
		ViewQuery query = new ViewQuery()
		          .designDocId("_design/Event")
		          .viewName("events_by_version");
		ViewResult result = dbClient.queryView(query.startKey(startKey).endKey(endKey)
				.includeDocs(true).limit(limit));
		List<JSONObject> results= new ArrayList<JSONObject>();
		for (ViewResult.Row row : result) {
		       JsonNode docNode = row.getDocAsNode();
		      JSONObject object= new JSONObject(docNode.toString());
		      results.add(object);
		     }
		return results;
	}

	public List<JSONObject> findClientsByServerVersion(long serverVersion, int limit) throws JSONException {
		ComplexKey startKey = ComplexKey.of(serverVersion + 1);
		ComplexKey endKey = ComplexKey.of(Long.MAX_VALUE);
		ViewQuery query = new ViewQuery()
		          .designDocId("_design/Client")
		          .viewName("events_by_version");
		ViewResult result = dbClient.queryView(query.startKey(startKey).endKey(endKey)
				.includeDocs(true).limit(limit));
		List<JSONObject> results= new ArrayList<JSONObject>();

		for (ViewResult.Row row : result) {
		       JsonNode docNode = row.getDocAsNode();
		      JSONObject object= new JSONObject(docNode.toString());
		      results.add(object);
		     }
		return results;
	}

}
