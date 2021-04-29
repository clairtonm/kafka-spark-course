# Image created to use Elasticsearch driver
FROM apache/superset
# Switching to root to install the required packages
USER root
# Example: installing the Elasticsearch driver
RUN pip install elasticsearch-dbapi
# Switching back to using the `superset` user
USER superset