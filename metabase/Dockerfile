FROM eclipse-temurin:17-jdk-jammy

ENV MB_PLUGINS_DIR=/home/plugins/

RUN mkdir -p /home/plugins

ADD https://downloads.metabase.com/v0.52.13/metabase.jar /home/metabase.jar
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12-b/duckdb.metabase-driver.jar /home/plugins/duckdb.metabase-driver.jar

RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar

CMD ["java", "-jar", "/home/metabase.jar"]