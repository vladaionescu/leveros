FROM aerospike/aerospike-server:latest

LABEL com.leveros.isleveros="true"

COPY ./etc /opt/aerospike/etc
RUN chown aerospike:aerospike /opt/aerospike/etc/aerospike.conf

CMD ["asd", "--config-file", "/opt/aerospike/etc/aerospike.conf"]
