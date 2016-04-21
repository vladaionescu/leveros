FROM leveros/base:latest

LABEL com.leveros.islevercontainer="true"

RUN echo '@edge http://nl.alpinelinux.org/alpine/edge/main' >> /etc/apk/repositories
RUN echo '@community http://nl.alpinelinux.org/alpine/edge/community' >> /etc/apk/repositories

RUN apk add --update \
    curl \
    libevent \
    make \
    nodejs@edge \
    python \
    tar \
    unzip \
    wget
RUN rm -rf /var/cache/apk/*

# This is really important to avoid Lever customers from running as root.
# Without ns remap it would mean that in case of a breakout they would have
# root access to the host.
RUN addgroup lever
RUN adduser -D -G lever -s /sbin/nologin lever
USER lever

# Port listening on for Lever RPCs.
EXPOSE 3837

# The customer code that will handle Lever RPCs.
VOLUME /leveros/custcode

WORKDIR /leveros/custcode
