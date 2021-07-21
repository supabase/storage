FROM mhart/alpine-node:14

# Install packages
WORKDIR /app
COPY . .
RUN npm ci 
RUN npm run build

EXPOSE 5000
CMD ["npm", "run", "start"]
