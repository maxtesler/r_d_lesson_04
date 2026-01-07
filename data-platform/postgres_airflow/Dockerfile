FROM postgres:14

RUN apt-get update && apt-get install -y gettext-base

COPY postgresql.conf /etc/postgresql/postgresql.conf
COPY init.sql.template /docker-entrypoint-initdb.d/init.sql.template
COPY init.sh /docker-entrypoint-initdb.d/init.sh

RUN chmod +x /docker-entrypoint-initdb.d/init.sh && \
    chown postgres:postgres /docker-entrypoint-initdb.d/init.sql.template /docker-entrypoint-initdb.d/init.sh

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]