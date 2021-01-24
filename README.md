# kafka-connect-smt-TopicRouter
A simple SMT to manipulate the index name using a document field.

"transforms": "topicRoute",
"transforms.topicRoute.type": "de.itcur.smt.TopicRouter",
"transforms.topicRoute.topic.appendix.field": "your-field"
