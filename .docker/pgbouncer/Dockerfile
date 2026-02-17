FROM alpine:3.20

RUN apk add --no-cache pgbouncer bash

RUN addgroup -S pgbouncer 2>/dev/null || true \
    && adduser -S -G pgbouncer pgbouncer 2>/dev/null || true \
    && mkdir -p /etc/pgbouncer /var/log/pgbouncer /var/run/pgbouncer \
    && chown -R pgbouncer:pgbouncer /etc/pgbouncer /var/log/pgbouncer /var/run/pgbouncer

COPY --chown=pgbouncer:pgbouncer pgbouncer.ini.template /etc/pgbouncer/pgbouncer.ini.template
COPY --chown=pgbouncer:pgbouncer entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 6432

USER pgbouncer

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD printf "SHOW VERSION;\n" | nc 127.0.0.1 6432 >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/entrypoint.sh"]
