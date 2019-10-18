# Build Stage
FROM node:10.16.3 AS build-stage

LABEL app="build-rabbitqpsexchange"
LABEL REPO="https://github.com/freethenation/rabbit-qps-exchange"

ADD . /opt/rabbitqpsexchange
WORKDIR /opt/rabbitqpsexchange

RUN npm install --unsafe-perm=true

CMD ["rabbit_qps_exchange.js", "start"]
