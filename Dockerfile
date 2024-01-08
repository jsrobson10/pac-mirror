FROM node:latest
WORKDIR /app

COPY package.json .
RUN npm install
COPY *.js .
COPY config.yaml .

VOLUME db
EXPOSE 80
CMD node mirror.js

