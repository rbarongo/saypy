# Vite 7 requires Node 20.19+/22.12+ — this project's own dev machine needed
# an nvm-installed Node for the same reason (the OS-default Node here was
# 18.x), so the base image is pinned above that floor deliberately.
FROM node:20-alpine AS base
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

# Dev stage — hot-reloading Vite dev server (used by docker-compose.yml).
# package.json's "dev" script already binds 0.0.0.0:5175 so it's reachable
# from outside the container without any extra flags here.
FROM base AS dev
EXPOSE 5175
CMD ["npm", "run", "dev"]

# Build stage — production static bundle. VITE_* build args are baked into
# the compiled JS at build time (Vite inlines import.meta.env.* at build
# time, not read at container runtime) — see App.jsx/api.js for the
# same-origin-port-8001 fallback these normally don't even need to be set.
FROM base AS build
ARG VITE_API_BASE_URL
ARG VITE_MEMBERS_API_BASE_URL
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
ENV VITE_MEMBERS_API_BASE_URL=$VITE_MEMBERS_API_BASE_URL
RUN npm run build

# Prod stage — serves the static build with Nginx.
FROM nginx:alpine AS prod
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
