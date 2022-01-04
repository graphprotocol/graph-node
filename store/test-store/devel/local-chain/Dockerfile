FROM node:16.13.1-alpine3.14

WORKDIR /hardhat

ADD package.json hardhat.config.js .
RUN npm install

EXPOSE 8545

CMD npm run chain