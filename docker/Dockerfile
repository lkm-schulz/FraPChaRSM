FROM apache/spark:3.4.1-scala2.12-java11-ubuntu

USER root

RUN mkdir -p /opt/contistuff && \
    mkdir -p /opt/contistuff/keyfile && \
    mkdir -p /opt/spark/conf

# COPY foo bar copies foo into bar
# COPY foo bar/ copies contents of foo into bar
# https://stackoverflow.com/questions/26504846/copy-directory-to-another-directory-using-add-command
COPY docker/picklejars /opt/contistuff
COPY docker/tpcds-bin /opt/contistuff
COPY docker/spark-conf /opt/spark/conf/
COPY target/scala-2.12/parquet-data-generator_2.12-1.0.jar /opt/contistuff

USER 185