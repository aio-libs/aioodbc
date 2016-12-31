FROM python:3.5.2-slim
RUN apt-get update && apt-get install -y \
    unixODBC wget g++ unixodbc-dev odbc-postgresql libmyodbc libsqlite-dev libtool build-essential && \
    odbcinst -i -d -f /usr/share/libmyodbc/odbcinst.ini && \
    wget http://archive.ubuntu.com/ubuntu/pool/universe/s/sqliteodbc/libsqliteodbc_0.9992-0.1_amd64.deb && \
    dpkg -i libsqliteodbc_0.9992-0.1_amd64.deb

ADD / /aioodbc
RUN pip install -e /aioodbc/ && \
    pip install -U pip setuptools && \
    pip install -r /aioodbc/requirements-dev.txt

RUN rm -rf /aioodbc && \
    rm libsqliteodbc_0.9992-0.1_amd64.deb && \
    apt-get purge -y g++ && \
    apt-get purge -y wget && \
    apt-get autoremove -y

VOLUME /aioodbc
WORKDIR /aioodbc
