elasticsearch-river-github
==========================

Elasticsearch river for GitHub data. Fetches all of the following for
a given GitHub repo:

* [events](http://developer.github.com/v3/activity/events/)
* [issues](http://developer.github.com/v3/issues/#list-issues-for-a-repository)
* [open pull requests](http://developer.github.com/v3/pulls/#list-pull-requests)
* [open milestones](http://developer.github.com/v3/issues/milestones/)
* [labels](http://developer.github.com/v3/issues/labels/)
* [collaborators](http://developer.github.com/v3/repos/collaborators/#list)

Works for private repos as well if you provide authentication.

##Easy install

Assuming you have elasticsearch's `bin` folder in your `PATH`:

```
plugin -i com.ubervu/elasticsearch-river-github/1.7.1
```

Otherwise, you have to find the directory yourself. It should be
`/usr/share/elasticsearch/bin` on Ubuntu.

##Adding the river

```bash
curl -XPUT localhost:9200/_river/my_gh_river/_meta -d '{
    "type": "github",
    "github": {
        "owner": "gabrielfalcao",
        "repository": "lettuce",
        "interval": 60,
        "authentication": {
            "username": "MYUSER", # or token
            "password": "MYPASSWORD" # or x-oauth-basic when using a token
        }
        "endpoint": "https://api.somegithub.com" # optional, use it only for non github.com
    }
}'
```

_interval_ is optional, given in seconds and changes how often the river looks for new data. Since 1.7.1 the default value has been reduced to one minute as we now only load issues and events that has changed, which should decrease API calls and improve the time to update quite significantly. The actual polling interval will be affected by GitHub's minimum allowed polling interval, which is normally 60 seconds, but may increase when servers are busy.

_authentication_ is optional and helps with the API rate limit (5000 requests/hour instead of 60 requests/hour) and when accessing private data. You can use your own GitHub credentials or a token. When using a token, fill in the token as the username and `x-oauth-basic` as the password, as the [docs](http://developer.github.com/v3/auth/#basic-authentication) mention.

If you do not use _authentication_, you may want to set _interval_ to a higher value, like 900 (every 15 minutes), as the GitHub rate limit will probably be breached when using low values. This is __not__ recommended if you require the GitHub events without holes, as Github only allows access to the last 300 events. In that case, authenticating is highly recommended. _This will probably change in a later version, at least for repositories without too much traffic, as we should be able to check for changes before loading most types of entries._

##Deleting the river

```
curl -XDELETE localhost:9200/_river/my_gh_river
```

##Indexes and types

The data will be stored in an index of format "%s&%s" % (owner, repo), i.e.
`gabrielfalcao&lettuce`.

For every API event type, there will be an elasticsearch type of the same name -
i.e. `ForkEvent`.

Issue data will be stored with the `IssueData` type. Pull request data will be stored
with the `PullRequestData` type. Milestone data will be stored with the `MilestoneData`
type.

