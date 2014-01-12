package org.elasticsearch.river.github;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonStreamParser;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

public class GitHubRiver extends AbstractRiverComponent implements River {

    private final String indexName;
    private final String typeName;
    private final Client client;

    @SuppressWarnings({"unchecked"})
    @Inject
    public GitHubRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        logger.info("Creating GitHub river");
        logger.info("-------------------------------------\n\n\n");

        indexName = riverName.name();
        typeName = "event";
        this.client = client;

        // get auth
        // get time interval
        // fire off thread with timed checks

        String index = "gabrielfalcao-lettuce";

        try {
            URL url = new URL("https://api.github.com/repos/gabrielfalcao/lettuce/events");
            InputStream input = url.openStream();
            JsonStreamParser jsp = new JsonStreamParser(new InputStreamReader(input));

            JsonArray array = (JsonArray) jsp.next();
            for (JsonElement e: array) {
                JsonObject obj = e.getAsJsonObject();
                String type = obj.get("type").getAsString();
                String id = obj.get("id").getAsString();
                IndexRequest req = new IndexRequest(index)
                        .type(type.toLowerCase())
                        .id(id).create(true)
                        .source(e.toString());
                client.index(req);
            }

            input.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        logger.info("Done\n\n");
    }

    @Override
    public void start() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public static void main(String[] args) {
        System.out.println("helo");

        try {
            URL url = new URL("https://api.github.com/repos/gabrielfalcao/lettuce/events");
            InputStream input = url.openStream();
            JsonStreamParser jsp = new JsonStreamParser(new InputStreamReader(input));

            JsonArray array = (JsonArray) jsp.next();
            for (JsonElement e: array) {
                JsonObject obj = e.getAsJsonObject();
                System.out.println(e.toString());
                String type = obj.get("type").toString();
            }

            input.close();
        } catch (IOException e) {}

    }
}

