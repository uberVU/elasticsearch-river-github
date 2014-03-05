package com.ubervu.river.github;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonStreamParser;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;


public class GitHubRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final String index;
    private final String repository;
    private final String owner;
    private final int interval;
    private String password;
    private String username;
    private DataStream dataStream;

    @SuppressWarnings({"unchecked"})
    @Inject
    public GitHubRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        if (!settings.settings().containsKey("github")) {
            throw new IllegalArgumentException("Need river settings - owner and repository.");
        }

        // get settings
        Map<String, Object> githubSettings = (Map<String, Object>) settings.settings().get("github");
        owner = XContentMapValues.nodeStringValue(githubSettings.get("owner"), null);
        repository = XContentMapValues.nodeStringValue(githubSettings.get("repository"), null);
        index = String.format("%s&%s", owner, repository);
        interval = XContentMapValues.nodeIntegerValue(githubSettings.get("interval"), 3600);

        // auth (optional)
        username = null;
        password = null;
        if (githubSettings.containsKey("authentication")) {
            Map<String, Object> auth = (Map<String, Object>) githubSettings.get("authentication");
            username = XContentMapValues.nodeStringValue(auth.get("username"), null);
            password = XContentMapValues.nodeStringValue(auth.get("password"), null);
        }

        logger.info("Created GitHub river.");
    }

    @Override
    public void start() {
        dataStream = new DataStream();
        dataStream.start();
        logger.info("Started GitHub river.");
    }

    @Override
    public void close() {
        dataStream.setRunning(false);
        logger.info("Stopped GitHub river.");
    }

    private class DataStream extends Thread {
        private boolean isRunning;

        @Inject
        public DataStream() {
            super("DataStream thread");
            isRunning = true;
        }

        private void indexResponse(URLConnection conn, String type) throws IOException {
            InputStream input = conn.getInputStream();
            JsonStreamParser jsp = new JsonStreamParser(new InputStreamReader(input));

            JsonArray array = (JsonArray) jsp.next();

            BulkProcessor bp = BulkProcessor.builder(client, new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                }
            }).build();

            IndexRequest req = null;
            for (JsonElement e: array) {
                if (type.equals("event")) {
                    req = indexEvent(e);
                } else if (type.equals("issue")) {
                    req = indexOther(e, "IssueData", true);
                } else if (type.equals("pullreq")) {
                    req = indexOther(e, "PullRequestData");
                } else if (type.equals("milestone")) {
                    req = indexOther(e, "MilestoneData", true);
                } else if (type.equals("label")) {
                    req = indexOther(e, "LabelData", true);
                }
                bp.add(req);
            }
            bp.close();

            input.close();
        }

        private IndexRequest indexEvent(JsonElement e) {
            JsonObject obj = e.getAsJsonObject();
            String type = obj.get("type").getAsString();
            String id = obj.get("id").getAsString();
            IndexRequest req = new IndexRequest(index)
                    .type(type)
                    .id(id).create(false) // we want to overwrite old items
                    .source(e.toString());
            return req;
        }

        private IndexRequest indexOther(JsonElement e, String type, boolean overwrite) {
            JsonObject obj = e.getAsJsonObject();
            String id = obj.get("id").getAsString();
            IndexRequest req = new IndexRequest(index)
                    .type(type)
                    .id(id).create(!overwrite)
                    .source(e.toString());
            return req;
        }

        private IndexRequest indexOther(JsonElement e, String type) {
            return indexOther(e, type, false);
        }

        private HashMap<String, String> parseHeader(String header) {
            // inspired from https://github.com/uberVU/elasticboard/blob/4ccdfd8c8e772c1dda49a29a7487d14b8d820762/data_processor/github.py#L73
            Pattern p = Pattern.compile("\\<([a-z/0-9:\\.\\?_&=]+page=([0-9]+))\\>;\\s*rel=\\\"([a-z]+)\\\".*");
            Matcher m = p.matcher(header);

            if (!m.matches()) {
                return null;
            }

            HashMap<String, String> data = new HashMap<String, String>();
            data.put("url", m.group(1));
            data.put("page", m.group(2));
            data.put("rel", m.group(3));

            return data;
        }

        private boolean morePagesAvailable(URLConnection response) {
            String link = response.getHeaderField("link");
            if (link == null || link.length() == 0) {
                return false;
            }

            HashMap<String, String> headerData = parseHeader(response.getHeaderField("link"));
            if (headerData == null) {
                return false;
            }

            String rel = headerData.get("rel");
            return rel.equals("next");
        }

        private String nextPageURL(URLConnection response) {
            HashMap<String, String> headerData = parseHeader(response.getHeaderField("link"));
            if (headerData == null) {
                return null;
            }
            return headerData.get("url");
        }

        private void addAuthHeader(URLConnection request) {
            if (username == null || password == null) {
                return;
            }
            String auth = String.format("%s:%s", username, password);
            String encoded = Base64.encodeBytes(auth.getBytes());
            request.setRequestProperty("Authorization", "Basic " + encoded);
        }

        private void getData(String fmt, String type) {
            try {
                URL url = new URL(String.format(fmt, owner, repository));
                URLConnection response = url.openConnection();
                addAuthHeader(response);
                indexResponse(response, type);

                while (morePagesAvailable(response)) {
                    url = new URL(nextPageURL(response));
                    response = url.openConnection();
                    addAuthHeader(response);
                    indexResponse(response, type);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        @Override
        public void run() {
            while (isRunning) {
                getData("https://api.github.com/repos/%s/%s/events?per_page=1000", "event");
                getData("https://api.github.com/repos/%s/%s/issues?per_page=1000", "issue");
                getData("https://api.github.com/repos/%s/%s/issues?state=closed&per_page=1000", "issue");
                getData("https://api.github.com/repos/%s/%s/labels?per_page=1000", "label");

                // delete pull req data - we are only storing open pull reqs
                // and when a pull request is closed we have no way of knowing;
                // this is why we have to delete them and reindex "fresh" ones
                DeleteByQueryResponse response = client.prepareDeleteByQuery(index)
                        .setQuery(termQuery("_type", "PullRequestData"))
                        .execute()
                        .actionGet();
                getData("https://api.github.com/repos/%s/%s/pulls", "pullreq");

                // same for milestones
                response = client.prepareDeleteByQuery(index)
                        .setQuery(termQuery("_type", "MilestoneData"))
                        .execute()
                        .actionGet();
                getData("https://api.github.com/repos/%s/%s/milestones?per_page=1000", "milestone");

                try {
                    Thread.sleep(interval * 1000); // needs milliseconds
                } catch (InterruptedException e) {}
            }
        }

        public void setRunning(boolean running) {
            isRunning = running;
        }
    }
}
