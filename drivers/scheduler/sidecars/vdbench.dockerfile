FROM openebs/tests-vdbench

COPY scripts/vdbench.sh bench_runner.sh

RUN chmod +x bench_runner.sh

CMD ["/bench_runner.sh"]