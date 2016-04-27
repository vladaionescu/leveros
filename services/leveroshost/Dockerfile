FROM leveros/base:latest

COPY ./leveroshost /leveros/bin/

EXPOSE 3500 3501 3502 3503 3838 6514
VOLUME /var/run/docker.sock

WORKDIR /leveros
ENTRYPOINT ["/leveros/bin/leveroshost"]
CMD []
