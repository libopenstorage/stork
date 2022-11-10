FROM alpine/git AS downloader
USER root
RUN cd /tmp && git clone https://github.com/logzio/elasticsearch-stress-test.git
FROM python:2
WORKDIR /usr/src/elasticsearch-stress-test
COPY --from=downloader /tmp/elasticsearch-stress-test/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY --chmod=777 scripts/elk-stack/elasticsearch_read.py ./
COPY --chmod=777 scripts/elk-stack/esread.sh ./