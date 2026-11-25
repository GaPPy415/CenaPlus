# Stage 1: Build the application
FROM oven/bun:1 AS build

WORKDIR /app/frontend

COPY frontend/package.json frontend/bun.lockb ./
RUN bun install --frozen-lockfile

COPY frontend/index.html frontend/vite.config.ts frontend/tsconfig*.json frontend/tailwind.config.ts frontend/postcss.config.js frontend/components.json ./
COPY frontend/src ./src
COPY frontend/public ./public
COPY shared /app/shared

RUN bun run build

# Stage 2: Serve with Nginx
FROM nginx:alpine

COPY --from=build /app/frontend/dist /usr/share/nginx/html
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
