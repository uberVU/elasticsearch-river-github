package com.ubervu.plugin.river.github;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import com.ubervu.river.github.GitHubRiverModule;

/**
 *
 */
public class GitHubRiverPlugin extends AbstractPlugin {

    @Inject
    public GitHubRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-github";
    }

    @Override
    public String description() {
        return "GitHub River Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("github", GitHubRiverModule.class);
    }
}
