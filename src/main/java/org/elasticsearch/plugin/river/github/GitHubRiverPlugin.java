package org.elasticsearch.plugin.river.github;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.github.GitHubRiverModule;

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
        return "GitHub Twitter Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("github", GitHubRiverModule.class);
    }
}
