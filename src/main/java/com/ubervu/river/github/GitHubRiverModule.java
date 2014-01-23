package com.ubervu.river.github;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 *
 */
public class GitHubRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(GitHubRiver.class).asEagerSingleton();
    }
}

