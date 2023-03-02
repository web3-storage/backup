FROM node:18.14.2-alpine

WORKDIR /usr/src/app
COPY package*.json *.js ./
RUN npm ci --omit=dev

COPY *.js *.json *.sh ./
CMD [ "npm", "start", "--silent"]
