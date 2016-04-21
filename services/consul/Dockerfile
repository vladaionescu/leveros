FROM leveros/base:latest

RUN apk add --update docker wget unzip \
    && rm -rf /var/cache/apk/*

# Install consul.
RUN bash -c "cd /usr/local/bin && wget https://releases.hashicorp.com/consul/0.6.4/consul_0.6.4_linux_amd64.zip && unzip consul_0.6.4_linux_amd64.zip && rm consul_0.6.4_linux_amd64.zip"
RUN bash -c "mkdir -p /consul/ui && cd /consul/ui && wget https://releases.hashicorp.com/consul/0.6.4/consul_0.6.4_web_ui.zip && unzip consul_0.6.4_web_ui.zip && rm consul_0.6.4_web_ui.zip"
RUN bash -c "mkdir -p /consul/data"

COPY conf.json /consul/conf/conf.json

EXPOSE 8300 8301 8301/udp 8302 8302/udp 8400 8500 8600 8600/udp
VOLUME /var/run/docker.sock

ENTRYPOINT ["/usr/local/bin/consul", "agent", "-ui-dir", "/consul/ui", "-data-dir", "/consul/data", "-client", "0.0.0.0", "-config-file", "/consul/conf/conf.json"]
CMD []
