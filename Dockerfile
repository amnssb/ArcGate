# ArcGate —— 多阶段构建：Go 编译 → 精简 alpine 运行层
# 构建镜像内不需要 .env / logs / data（见 .dockerignore），配置全部用环境变量注入

# ---- 构建阶段 ----
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
# 纯标准库、无 CGO：静态编译，产物无任何运行时依赖
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/arcgate .

# ---- 运行阶段 ----
FROM alpine:3.20
# ca-certificates：HTTPS 调用站点 Webhook；tzdata：日志本地时区
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S arcgate -g 1000 \
    && adduser -S arcgate -u 1000 \
    && mkdir -p /app/logs /app/data \
    && chown -R arcgate:arcgate /app
WORKDIR /app
COPY --from=build /out/arcgate /usr/local/bin/arcgate

# 容器内必须监听所有网卡（默认 127.0.0.1 仅本机可达）
ENV ARC_LISTEN_ADDR=0.0.0.0:3002 \
    ARC_LOG_DIR=/app/logs \
    ARC_PENDING_DIR=/app/data

VOLUME ["/app/logs", "/app/data"]
EXPOSE 3002

USER arcgate
ENTRYPOINT ["arcgate"]
