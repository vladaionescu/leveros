FROM leveros/ubuntubase:latest

COPY ./leveroshost /leveros/bin/

EXPOSE 3500 3501 3838 8080
VOLUME /var/run/docker.sock

WORKDIR /leveros
ENTRYPOINT ["/leveros/bin/leveroshost"]
CMD []
