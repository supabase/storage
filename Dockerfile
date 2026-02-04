# Base stage for shared environment setup
FROM node:24-alpine3.23 AS base
RUN apk add --no-cache g++ make python3
WORKDIR /app
COPY package.json package-lock.json ./

# Dependencies stage - install and cache all dependencies
FROM base AS dependencies
# Copy patches before npm ci so postinstall can apply them
COPY patches ./patches
RUN npm ci
# Cache the installed node_modules for later stages
RUN cp -R node_modules /node_modules_cache

# Build stage - use cached node_modules for building the application
FROM base AS build
COPY --from=dependencies /node_modules_cache ./node_modules
COPY . .
RUN npm run build

# Production dependencies stage - prune dev dependencies from cached node_modules
FROM base AS production-deps
COPY --from=dependencies /node_modules_cache ./node_modules
# Use npm prune to remove dev dependencies while keeping compiled native modules and patches
RUN npm prune --omit=dev

# Final stage - for the production build
FROM base AS final
ARG VERSION
ENV VERSION=$VERSION
COPY migrations migrations

# Copy production node_modules from the production dependencies stage
COPY --from=production-deps /app/node_modules node_modules
# Copy build artifacts from the build stage
COPY --from=build /app/dist dist

EXPOSE 5000
CMD ["node", "dist/start/server.js"]