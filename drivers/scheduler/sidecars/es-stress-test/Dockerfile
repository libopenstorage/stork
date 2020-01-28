FROM alpine/git AS downloader

RUN cd /tmp && git clone https://github.com/logzio/elasticsearch-stress-test.git

FROM python:2

WORKDIR /usr/src/elasticsearch-stress-test

COPY --from=downloader /tmp/elasticsearch-stress-test/elasticsearch-stress-test.py ./
COPY --from=downloader /tmp/elasticsearch-stress-test/requirements.txt ./
COPY esload.sh ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "./elasticsearch-stress-test.py" ]

