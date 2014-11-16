package com.ubervu.river.github;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonStreamParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;


public class GitHubRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final String index;
    private final String repository;
    private final String owner;
    private final int userRequestedInterval;
    private final String endpoint;

    private String password;
    private String username;
    private DataStream dataStream;
    private String eventETag = null;
    private int pollInterval = 60;

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
        userRequestedInterval = XContentMapValues.nodeIntegerValue(githubSettings.get("interval"), 60);

        // auth (optional)
        username = null;
        password = null;
        if (githubSettings.containsKey("authentication")) {
            Map<String, Object> auth = (Map<String, Object>) githubSettings.get("authentication");
            username = XContentMapValues.nodeStringValue(auth.get("username"), null);
            password = XContentMapValues.nodeStringValue(auth.get("password"), null);
        }

        // endpoint (optional - default to github.com)
        endpoint = XContentMapValues.nodeStringValue(githubSettings.get("endpoint"), "https://api.github.com");

        logger.info("Created GitHub river.");
    }

    @Override
    public void start() {
        // create the index explicitly so we can use the whitespace tokenizer
        //   because there are usernames like "user-name" and we want those
        //   to be treated as just one term
        try {
            Settings indexSettings = ImmutableSettings.settingsBuilder().put("analysis.analyzer.default.tokenizer", "whitespace").build();
            client.admin().indices().prepareCreate(index).setSettings(indexSettings).execute().actionGet();
            logger.info("Created index.");
        } catch (IndexAlreadyExistsException e) {
            logger.info("Index already created");
        } catch (Exception e) {
            logger.error("Exception creating index.", e);
        }
        dataStream = new DataStream();
        dataStream.start();
        logger.info("Started GitHub river.");
    }

    @Override
    public void close() {
        dataStream.setRunning(false);
        dataStream.interrupt();
        logger.info("Stopped GitHub river.");
    }

    private class DataStream extends Thread {
        private volatile boolean isRunning;

        @Inject
        public DataStream() {
            super("DataStream thread");
            isRunning = true;
        }

        private boolean checkAndUpdateETag(HttpURLConnection conn) throws IOException {
            if (eventETag != null) {
                conn.setRequestProperty("If-None-Match", eventETag);
            }

            String xPollInterval = conn.getHeaderField("X-Poll-Interval");
            if (xPollInterval != null) {
                logger.debug("Next GitHub specified minimum polling interval is {} s", xPollInterval);
                pollInterval = Integer.parseInt(xPollInterval);
            }

            if (conn.getResponseCode() == 304) {
                logger.debug("304 {}", conn.getResponseMessage());
                return false;
            }

            String eTag = conn.getHeaderField("ETag");
            if (eTag != null) {
                logger.debug("New eTag: {}", eTag);
                eventETag = eTag;
            }

            return true;
        }

        private boolean indexResponse(HttpURLConnection conn, String type) {
            InputStream input;
            try {
                input = conn.getInputStream();
            } catch (IOException e) {
                logger.info("Exception encountered (403 usually is rate limit exceeded): ", e);
                return false;
            }
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

            boolean continueIndexing = true;

            IndexRequest req = null;
            for (JsonElement e: array) {
                if (type.equals("event")) {
                    req = indexEvent(e);
                    if (req == null) {
                        continueIndexing = false;
                        logger.debug("Found existing event, all remaining events has already been indexed");
                        break;
                    }
                } else if (type.equals("issue")) {
                    req = indexOther(e, "IssueData", true);
                } else if (type.equals("pullreq")) {
                    req = indexOther(e, "PullRequestData");
                } else if (type.equals("milestone")) {
                    req = indexOther(e, "MilestoneData");
                } else if (type.equals("label")) {
                    req = indexOther(e, "LabelData");
                } else if (type.equals("collaborator")) {
                    req = indexOther(e, "CollaboratorData");
                }
                bp.add(req);
            }
            bp.close();

            try {
                input.close();
            } catch (IOException e) {
                logger.warn("Couldn't close connection?", e);
            }

            return continueIndexing;
        }

        private boolean isEventIndexed(String id) {
            return client.prepareGet(index, null, id).get().isExists();
        }

        private IndexRequest indexEvent(JsonElement e) {
            JsonObject obj = e.getAsJsonObject();
            String type = obj.get("type").getAsString();
            String id = obj.get("id").getAsString();

            if (isEventIndexed(id)) {
                return null;
            }

            IndexRequest req = new IndexRequest(index)
                    .type(type)
                    .id(id).create(false) // we want to overwrite old items
                    .source(e.toString());
            return req;
        }

        private IndexRequest indexOther(JsonElement e, String type, boolean overwrite) {
            JsonObject obj = e.getAsJsonObject();

            // handle objects that don't have IDs (i.e. labels)
            // set the ID to the MD5 hash of the string representation
            String id;
            if (obj.has("id")) {
                id = obj.get("id").getAsString();
            } else {
                id = DigestUtils.md5Hex(e.toString());
            }

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

        private void addAuthHeader(URLConnection connection) {
            if (username == null || password == null) {
                return;
            }
            String auth = String.format("%s:%s", username, password);
            String encoded = Base64.encodeBytes(auth.getBytes());
            connection.setRequestProperty("Authorization", "Basic " + encoded);
        }

        private boolean getData(String fmt, String type) {
            return getData(fmt, type, null);
        }

        private boolean getData(String fmt, String type, String since) {
            try {
                URL url;
                if (since != null) {
                    url = new URL(String.format(fmt, owner, repository, since));
                } else {
                    url = new URL(String.format(fmt, owner, repository));
                }
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                addAuthHeader(connection);
                if (type.equals("event")) {
                    boolean modified = checkAndUpdateETag(connection);
                    if (!modified) {
                        return false;
                    }
                }
                boolean continueIndexing = indexResponse(connection, type);

                while (continueIndexing && morePagesAvailable(connection)) {
                    url = new URL(nextPageURL(connection));
                    connection = (HttpURLConnection) url.openConnection();
                    addAuthHeader(connection);
                    continueIndexing = indexResponse(connection, type);
                }
            } catch (Exception e) {
                logger.error("Exception in getData", e);
            }

            return true;
        }

        private void deleteByType(String type) {
            client.prepareDeleteByQuery(index)
                    .setQuery(termQuery("_type", type))
                    .execute()
                    .actionGet();
        }

        /**
         * Gets the creation data of the single newest entry.
         *
         * @return ISO8601 formatted time of most recent entry, or null on empty or error.
         */
        private String getMostRecentEntry() {
            long totalEntries = client.prepareCount(index).setQuery(matchAllQuery()).execute().actionGet().getCount();
            if (totalEntries > 0) {
                FilteredQueryBuilder updatedAtQuery = QueryBuilders
                        .filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.existsFilter("created_at"));
                FieldSortBuilder updatedAtSort = SortBuilders.fieldSort("created_at").order(SortOrder.DESC);

                SearchResponse response = client.prepareSearch(index)
                        .setQuery(updatedAtQuery)
                        .addSort(updatedAtSort)
                        .setSize(1)
                        .execute()
                        .actionGet();

                String createdAt = (String) response.getHits().getAt(0).getSource().get("created_at");
                logger.debug("Most recent event was created at {}", createdAt);
                return createdAt;
            } else {
                // getData will get all data on a null.
                logger.info("No existing entries, assuming first run");
                return null;
            }
        }

        @Override
        public void run() {
            while (isRunning) {
                // Must be read before getting new events.
                String mostRecentEntry = getMostRecentEntry();

                logger.debug("Checking for events");
                if (getData(endpoint + "/repos/%s/%s/events?per_page=1000",
                        "event")) {
                    logger.debug("First run or new events found, fetching rest of the data");
                    if (mostRecentEntry != null) {
                        getData(endpoint + "/repos/%s/%s/issues?state=all&per_page=1000&since=%s",
                                "issue", mostRecentEntry);
                    } else {
                        getData(endpoint + "/repos/%s/%s/issues?state=all&per_page=1000",
                                "issue");
                    }
                    // delete pull req data - we are only storing open pull reqs
                    // and when a pull request is closed we have no way of knowing;
                    // this is why we have to delete them and reindex "fresh" ones
                    deleteByType("PullRequestData");
                    getData(endpoint + "/repos/%s/%s/pulls", "pullreq");

                    // same for milestones
                    deleteByType("MilestoneData");
                    getData(endpoint + "/repos/%s/%s/milestones?per_page=1000", "milestone");

                    // collaborators
                    deleteByType("CollaboratorData");
                    getData(endpoint + "/repos/%s/%s/collaborators?per_page=1000", "collaborator");

                    // and for labels - they have IDs based on the MD5 of the contents, so
                    // if a property changes, we get a "new" document
                    deleteByType("LabelData");
                    getData(endpoint + "/repos/%s/%s/labels?per_page=1000", "label");
                } else {
                    logger.debug("No new events found");
                }
                try {
                    int waitTime = Math.max(pollInterval, userRequestedInterval) * 1000;
                    logger.debug("Waiting {} ms before polling for new events", waitTime);
                    Thread.sleep(waitTime); // needs milliseconds
                } catch (InterruptedException e) {
                    logger.info("Wait interrupted, river was probably stopped");
                }
            }
        }

        public void setRunning(boolean running) {
            isRunning = running;
        }
    }
}
