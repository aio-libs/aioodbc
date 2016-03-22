FROM python:3.5.1
RUN apt-get update && apt-get install -y \
    unixODBC unixodbc-dev odbc-postgresql libmyodbc libsqlite-dev && \
    odbcinst -i -d -f /usr/share/libmyodbc/odbcinst.ini && \
    wget http://http.us.debian.org/debian/pool/main/s/sqliteodbc/libsqliteodbc_0.9992-0.1_amd64.deb && \
    dpkg -i libsqliteodbc_0.9992-0.1_amd64.deb && \
    apt-get autoremove -y

ADD / /aioodbc
RUN pip install -e /aioodbc/
RUN pip install -Ur /aioodbc/requirements-dev.txt
RUN rm -rf /aioodbc

VOLUME /aioodbc
WORKDIR /aioodbc
