# Docker 构建速度优化指南

## 🚀 已实施的优化

### 1. **路径过滤** ✅
```yaml
paths:
  - 'hpf-audit/**'
  - 'hpf-platform/**'
  - 'hpf-common/**'
```
**效果**: 只修改文档或其他无关文件时，**跳过构建**，节省 6-10 分钟

### 2. **GitHub Actions 缓存** ✅
```yaml
cache-from: type=gha
cache-to: type=gha,mode=max
```
**效果**: 第2次构建时，**加速 50-70%**
- 首次构建: 1m30s
- 缓存命中: 30-45s

### 3. **BuildKit 内联缓存** ✅
```yaml
build-args: |
  BUILDKIT_INLINE_CACHE=1
```
**效果**: 优化层缓存，**再提速 10-15%**

---

## 📊 预期速度对比

| 场景 | 之前 | 优化后 | 提升 |
|------|------|--------|------|
| **首次构建** | 1m30s × 4 = 6min | 1m30s × 4 = 6min | - |
| **缓存命中** | 1m30s × 4 = 6min | 30s × 4 = **2min** | **66%** ⚡ |
| **只改文档** | 6min | **跳过** | **100%** 🎯 |

---

## 🔧 进一步优化方案

### 方案 A: 多阶段构建优化（高级）

修改 `Dockerfile` 使用多阶段：

```dockerfile
# Stage 1: 基础镜像（变化少，缓存稳定）
FROM python:3.9-slim AS base
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Stage 2: 依赖安装（单独层，缓存友好）
FROM base AS deps
COPY hpf-common/requirements.txt /tmp/common-requirements.txt
RUN pip install -r /tmp/common-requirements.txt --no-cache-dir

# Stage 3: 应用代码（变化频繁）
FROM deps AS final
COPY hpf-common /app/hpf-common
COPY hpf-platform /app/hpf-platform
RUN pip install -e /app/hpf-platform
```

**预期加速**: 代码变更时，**跳过依赖安装**，再快 40%

---

### 方案 B: 条件构建（智能触发）

只构建**变化的镜像**：

```yaml
# 新建 .github/workflows/build-strategy.yml
jobs:
  detect-changes:
    runs-on: ubuntu-latest
    outputs:
      audit-backend: ${{ steps.filter.outputs.audit-backend }}
      audit-frontend: ${{ steps.filter.outputs.audit-frontend }}
      platform: ${{ steps.filter.outputs.platform }}
    steps:
      - uses: dorny/paths-filter@v2
        id: filter
        with:
          filters: |
            audit-backend:
              - 'hpf-audit/backend/**'
            audit-frontend:
              - 'hpf-audit/frontend/**'
            platform:
              - 'hpf-platform/**'
              - 'hpf-common/**'
  
  build:
    needs: detect-changes
    if: needs.detect-changes.outputs.platform == 'true'
    # 只构建变化的镜像
```

**效果**: 只改一个服务，**只构建一个镜像**
- 之前: 4个镜像 = 6分钟
- 现在: 1个镜像 = **1.5分钟** (节省 75%)

---

### 方案 C: 本地构建（超快）

开发时使用本地构建 + 手动推送：

```bash
# 本地构建（利用Mac多核）
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --cache-from type=registry,ref=ghcr.io/xiaoguan521/hpf-ml-trainer:latest \
  --cache-to type=inline \
  --push \
  -f hpf-platform/Dockerfile \
  -t ghcr.io/xiaoguan521/hpf-ml-trainer:dev .
```

**本地速度**: 30秒 - 1分钟（M1/M2 Mac）

---

## 🎯 推荐策略

### 日常开发
```bash
# 使用带路径过滤的自动构建
git push  # 只改了文档 → 跳过构建 ✅
git push  # 改了代码 → 自动构建（缓存加速）✅
```

### 快速迭代（紧急）
```bash
# 本地构建 + 推送
docker buildx build --push ...
# 30秒完成！
```

### 定期优化
- 每周清理一次旧镜像（GitHub Packages 有免费额度限制）
- 每月Review Dockerfile，减少层数

---

## 📈 监控构建性能

访问: https://github.com/xiaoguan521/hpf-intelligent-audit/actions

**查看指标**:
- ⏱️ Duration（构建时长）
- 💾 Cache hit rate（缓存命中率）
- 📊 Build size（镜像大小）

**目标**:
- 缓存命中率 > 80%
- 平均构建时间 < 1分钟
- 镜像大小 < 500MB

---

## 🚨 故障排查

### 缓存未生效？
```yaml
# 强制清除缓存
- name: Clear cache
  run: gh cache delete --all
```

### 构建过慢？
```bash
# 检查镜像大小
docker images | grep hpf-ml-trainer

# 减小镜像（使用 .dockerignore）
echo "*.md\n.git\ntests/" > .dockerignore
```
