FROM python:3.11-slim-bookworm AS base

RUN apt update -y \
&& apt install -y \
    postgresql-client \
    # mysqlclient build requirements
    python3-dev default-libmysqlclient-dev build-essential python3-pkgconfig

WORKDIR /opt/sqly

COPY setup.py setup.json README.md ./

FROM base AS test
RUN pip install -e .[dev,test,migration]
COPY ./ ./

CMD ["bash"]
