FROM node:14-alpine
RUN apk add --no-cache g++ make python3
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --production

FROM node:14-alpine
RUN apk add --no-cache g++ make python3
WORKDIR /app
COPY . .
RUN npm ci
RUN npm run build

FROM node:14-alpine
RUN npm install -g pm2
WORKDIR /app
COPY migrations migrations
COPY ecosystem.config.js package.json .
COPY --from=0 /app/node_modules node_modules
COPY --from=1 /app/dist dist
EXPOSE 5000
CMD ["pm2-runtime", "ecosystem.config.js"]
