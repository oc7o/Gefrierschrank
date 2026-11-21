ARG ES_VERSION=8.17.0
FROM docker.elastic.co/elasticsearch/elasticsearch:${ES_VERSION}
RUN bin/elasticsearch-plugin install --batch repository-s3
RUN bin/elasticsearch-keystore create
RUN echo "minioadmin" | bin/elasticsearch-keystore add -f --stdin s3.client.default.access_key
RUN echo "minioadmin" | bin/elasticsearch-keystore add -f --stdin s3.client.default.secret_key
