elasticsearch-river-github
==========================

Elasticsearch river for GitHub data. Fetches all of the following for
a given GitHub repo:

* [events](http://developer.github.com/v3/activity/events/)
* [issues](http://developer.github.com/v3/issues/#list-issues-for-a-repository)
* [open pull requests](http://developer.github.com/v3/pulls/#list-pull-requests)
* [open milestones](http://developer.github.com/v3/issues/milestones/)
* [labels](http://developer.github.com/v3/issues/labels/)

Works for private repos as well if you provide authentication.

##Easy install

Assuming you have elasticsearch's `bin` folder in your `PATH`:

```
plugin -i com.ubervu/elasticsearch-river-github/1.6.2
```

Otherwise, you have to find the directory yourself. It should be
`/usr/share/elasticsearch/bin` on Ubuntu.

##Adding the river

```
curl -XPUT localhost:9200/_river/gh_river/_meta -d '{
    "type": "github",
    "github": {
        "owner": "gabrielfalcao",
        "repository": "lettuce",
        "interval": 3600,
        "authentication": {
            "username": "MYUSER",
            "password": "MYPASSWORD"
        }
    }
}'
```

Interval is given in seconds and it changes how often the river looks for new data.

The authentication bit is optional.

##Deleting the river

```
curl -XDELETE localhost:9200/_river/gh_river
```

##Indexes and types

The data will be stored in an index of format "%s&%s" % (owner, repo), i.e.
`gabrielfalcao&lettuce`.

For every API event type, there will be an elasticsearch type of the same name -
i.e. `ForkEvent`.

Issue data will be stored with the `IssueData` type. Pull request data will be stored
with the `PullRequestData` type. Milestone data will be stored with the `MilestoneData`
type.

