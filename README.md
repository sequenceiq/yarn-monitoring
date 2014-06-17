yarn-monitoring
===============

Repisitory to monitor Hadoop YARN with R and generate metrics/charts. Complementary to Apache Ambari - monitors the MR jobs using YARN HistoryServer.
All the metrics are collected through the Timeline Server (previously also called Generic Application History Server) [REST API](http://hadoop.apache.org/docs/r2.4.0/hadoop-yarn/hadoop-yarn-site/TimelineServer.html).

This serves has two responsibilities:

* Generic information about completed applications: Generic information includes application level data like queue-name, user information etc in the ApplicationSubmissionContext, list of application-attempts that ran for an application, information about each application-attempt, list of containers run under each application-attempt, and information about each container. Generic data is stored by ResourceManager to a history-store (default implementation on a file-system) and used by the web-UI to display information about completed applications.

* Per-framework information of running and completed applications: Per-framework information is completely specific to an application or framework. For example, Hadoop MapReduce framework can include pieces of information like number of map tasks, reduce tasks, counters etc. Application developers can publish the specific information to the Timeline server via TimelineClient from within a client, the ApplicationMaster and/or the application's containers. This information is then queryable via REST APIs for rendering by application/framework specific UIs.

All runs are tracked for statistics.
