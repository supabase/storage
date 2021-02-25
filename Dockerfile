FROM mhart/alpine-node:12

# Install packages
WORKDIR /app
COPY . .
RUN npm ci 
RUN npm run build

EXPOSE 8080
CMD ["npm", "start"]
