# ==========================================
# hpf-audit-frontend Dockerfile
# ==========================================

# Stage 1: Build React App
FROM node:18-alpine AS builder
WORKDIR /app
COPY hpf-audit/frontend/package.json hpf-audit/frontend/package-lock.json* ./
RUN npm install
COPY hpf-audit/frontend ./
RUN npm run build

# Stage 2: Serve with Nginx
FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY hpf-audit/frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
