FROM python:3.6.1-slim

# configure apt to install minimal dependencies in non-interactive mode.
ENV DEBIAN_FRONTEND noninteractive
RUN echo 'APT::Install-Recommends "false";' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests "false";' >> /etc/apt/apt.conf

RUN apt-get update && apt-get install -y \
    unixODBC wget g++ unixodbc-dev odbc-postgresql libmyodbc libsqlite-dev libtool build-essential && \
    odbcinst -i -d -f /usr/share/libmyodbc/odbcinst.ini && \
    wget http://archive.ubuntu.com/ubuntu/pool/universe/s/sqliteodbc/libsqliteodbc_0.9992-0.1_amd64.deb && \
    dpkg -i libsqliteodbc_0.9992-0.1_amd64.deb

ADD / /aioodbc
RUN pip install -e /aioodbc/ && \
    pip install -U pip setuptools && \
    pip install -r /aioodbc/requirements-dev.txt

# with --squash option in docker build, this will reduce the final image size a bit.
RUN rm -rf /aioodbc && \
    rm libsqliteodbc_0.9992-0.1_amd64.deb && \
    apt-get purge -y g++ && \
    apt-get purge -y wget && \
    apt-get autoremove -y

VOLUME /aioodbc
WORKDIR /aioodbc
