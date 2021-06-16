# NOTE: stretch was very crashy when using https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.16-linux-debian9-x86-64bit.tar.gz
FROM python:3.7-slim-buster

# configure apt to install minimal dependencies in non-interactive mode.
ENV DEBIAN_FRONTEND noninteractive
RUN echo 'APT::Install-Recommends "false";' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests "false";' >> /etc/apt/apt.conf

RUN apt-get update && apt-get install -y \
    unixodbc wget g++ unixodbc-dev odbc-postgresql libsqlite-dev libtool build-essential && \
    wget http://archive.ubuntu.com/ubuntu/pool/universe/s/sqliteodbc/libsqliteodbc_0.9992-0.1_amd64.deb && \
    dpkg -i libsqliteodbc_0.9992-0.1_amd64.deb

ADD https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.25-linux-glibc2.12-x86-64bit.tar.gz /
RUN tar -xzf /mysql-connector-odbc-8.0.25-linux-glibc2.12-x86-64bit.tar.gz && \
    echo "[MySQL]" >> /etc/odbcinst.ini && \
    echo "Driver=/mysql-connector-odbc-8.0.25-linux-glibc2.12-x86-64bit/lib/libmyodbc8a.so" >> /etc/odbcinst.ini

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

WORKDIR /aioodbc
