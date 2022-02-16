FROM k8s.gcr.io/kubernetes-kafka:1.0-10.2.1

RUN mkdir -p /opt/kafka/agent

RUN wget -O /opt/kafka/agent/jmx_prometheus_javaagent-0.16.1.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar

COPY scripts/kafka/kafka_broker.yaml /opt/kafka/config