package org.elasticsearch.river.github;

import org.eclipse.egit.github.core.Repository;
import org.eclipse.egit.github.core.RepositoryId;
import org.eclipse.egit.github.core.client.EventFormatter;
import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.client.PageIterator;
import org.eclipse.egit.github.core.event.Event;
import org.eclipse.egit.github.core.event.EventPayload;
import org.eclipse.egit.github.core.event.IssuesPayload;
import org.eclipse.egit.github.core.service.EventService;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.util.Collection;

public class GitHubRiver extends AbstractRiverComponent implements River {

    private final String indexName;
    private final String typeName;
    private final GitHubClient github;
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

        github = new GitHubClient();

        EventService eventService = new EventService();
        RepositoryId repo = new RepositoryId("rosedu", "wouso");
        PageIterator<Event> iterator = eventService.pageEvents(repo);
        while (iterator.hasNext()) {
            Collection<Event> events = iterator.next();
            for (Event e: events) {

                EventPayload payload = e.getPayload();
                if (payload.getClass().getName().contains("IssuesPayload")) {
                    IssuesPayload p = (IssuesPayload) payload;
                    logger.info(p.getAction() + " -- #" + p.getIssue().getNumber());
                }
                EventFormatter formatter = new EventFormatter();
            }
        }
    }

    @Override
    public void start() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}

