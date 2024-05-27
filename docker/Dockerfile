FROM apache/spark:3.4.1-scala2.12-java11-ubuntu

USER root

RUN mkdir -p /opt/sparkbench && \
    mkdir -p /opt/sparkbench/keyfile && \
    mkdir -p /opt/spark/conf && \
    chown -R spark:spark /opt/sparkbench && \
    chmod -R 777 /opt/sparkbench

# COPY foo bar/foo copies foo into bar
# COPY foo bar/ copies contents of foo into bar
# https://stackoverflow.com/questions/26504846/copy-directory-to-another-directory-using-add-command
COPY docker/jars /opt/sparkbench/jars
COPY docker/tpcds-bin /opt/sparkbench/tpcds-bin
COPY spark-defaults.conf /opt/spark/conf/
COPY target/scala-2.12/parquet-data-generator_2.12-1.0.jar /opt/sparkbench/
COPY docker/entrypoint.sh /opt/sparkbench/entrypoint.sh
COPY docker/queries /opt/sparkbench/queries

# Only use outside google cloud if accessing google cloud storage
# ENTRYPOINT [ "/opt/sparkbench/entrypoint.sh" ]
ENTRYPOINT [ "/opt/entrypoint.sh" ]

USER 185
