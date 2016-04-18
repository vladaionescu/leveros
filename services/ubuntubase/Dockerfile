FROM ubuntu:latest

LABEL com.leveros.isleveros="true"

RUN apt-get update
RUN apt-get dist-upgrade -y
RUN apt-get install -y \
    software-properties-common \
    apt-transport-https

# Add docker repo (for using docker client for health checking).
RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 58118E89F3A912897C070ADBF76221572C52609D
RUN add-apt-repository "deb https://apt.dockerproject.org/repo ubuntu-trusty main"

RUN apt-get update
RUN apt-get dist-upgrade -y
RUN apt-get install -y \
    wget \
    unzip \
    docker-engine
