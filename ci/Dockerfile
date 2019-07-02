FROM python:3.6.8-slim-stretch

# configure apt to install minimal dependencies in non-interactive mode.
ENV DEBIAN_FRONTEND noninteractive
RUN echo 'APT::Install-Recommends "false";' >> /etc/apt/apt.conf; \
    echo 'APT::Install-Suggests "false";' >> /etc/apt/apt.conf

# jessie-updates is now archived
RUN apt-get update && apt-get install -y \
    unixodbc wget g++ unixodbc-dev odbc-postgresql libtool build-essential libsqliteodbc

# build and install mysqlodbc
# https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-installation-binary-unix-tarball.html
RUN wget -q https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.16-linux-debian9-x86-64bit.tar.gz -P /tmp/ && \
    tar -C /tmp -zxvpf /tmp/mysql-connector-odbc-*.tar.gz && \
    cp /tmp/mysql-connector-odbc*/bin/* /usr/local/bin/ && \
    cp /tmp/mysql-connector-odbc*/lib/* /usr/local/lib/ && \
    myodbc-installer -a -d -n "MySQL" -t "Driver=/usr/local/lib/libmyodbc8w.so" && \
    myodbc-installer -a -d -n "MySQL ODBC 8.0" -t "Driver=/usr/local/lib/libmyodbc8a.so"

RUN sed -i 's/libsqliteodbc\.so/libsqlite3odbc\.so/g' /etc/odbcinst.ini && \
    odbcinst -i -d -f /etc/odbcinst.ini

ADD / /aioodbc
RUN pip install -e /aioodbc/ && \
    pip install -U pip setuptools && \
    pip install -r /aioodbc/requirements-dev.txt

# with --squash option in docker build, this will reduce the final image size a bit.
RUN rm -rf /aioodbc && \
    apt-get purge -y g++ && \
    apt-get purge -y wget && \
    apt-get autoremove -y

VOLUME /aioodbc
WORKDIR /aioodbc
