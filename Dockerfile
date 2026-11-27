# Harbor 多阶段构建：CGO_ENABLED=0 静态二进制（modernc.org/sqlite 纯 Go，无 cgo）。
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/harbor .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S harbor \
    && adduser -S -G harbor harbor \
    && mkdir -p /data \
    && chown -R harbor:harbor /data
WORKDIR /app
COPY --from=build /out/harbor /usr/local/bin/harbor
COPY harbor.yaml /app/harbor.yaml
ENV PORT=8080
EXPOSE 8080
USER harbor
# 配置来源优先级：HARBOR_CONFIG 环境变量（完整 YAML）> /app/harbor.yaml（${ENV} 占位）。
# Zeabur 将自动注入 PORT，请为服务配置持久卷并设置 HARBOR_DB_PATH / HARBOR_KEY_PATH 指向卷。
CMD ["harbor", "--config", "/app/harbor.yaml"]
