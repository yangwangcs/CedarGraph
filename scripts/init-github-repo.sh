#!/bin/bash
# CedarGraph GitHub 仓库初始化脚本
# 使用方式: ./init-github-repo.sh <你的-github-用户名>

set -e

GITHUB_USER=${1:-}
if [ -z "$GITHUB_USER" ]; then
    echo "❌ 请提供 GitHub 用户名"
    echo "用法: ./init-github-repo.sh <github-username>"
    exit 1
fi

echo "🌲 CedarGraph GitHub 仓库初始化"
echo "================================"
echo "GitHub 用户: $GITHUB_USER"
echo ""

# 检查 git
if ! command -v git &> /dev/null; then
    echo "❌ git 未安装，请先安装 git"
    exit 1
fi

# 检查 gh CLI
if ! command -v gh &> /dev/null; then
    echo "⚠️  建议安装 GitHub CLI (gh) 以简化操作:"
    echo "    brew install gh"
    echo ""
    echo "或手动在 GitHub 网站创建仓库后运行此脚本"
fi

cd "$(dirname "$0")/.."
PROJECT_ROOT=$(pwd)
echo "📁 项目目录: $PROJECT_ROOT"
echo ""

# 初始化 git 仓库
echo "📦 步骤 1: 初始化 Git 仓库"
if [ ! -d ".git" ]; then
    git init
    echo "✅ Git 仓库已初始化"
else
    echo "✅ Git 仓库已存在"
fi

# 配置 git
echo ""
echo "⚙️  步骤 2: 配置 Git"
if [ -z "$(git config user.name)" ]; then
    git config user.name "CedarGraph Developer"
fi
if [ -z "$(git config user.email)" ]; then
    git config user.email "dev@cedargraph.io"
fi
echo "✅ Git 配置完成"

# 创建 .gitignore
echo ""
echo "📝 步骤 3: 创建 .gitignore"
cat > .gitignore << 'EOF'
# Build directories
build/
cmake-build-*/
out/

# IDE
.idea/
.vscode/
*.swp
*.swo
*~
.DS_Store

# Compiled files
*.o
*.a
*.so
*.dylib
*.exe
*.dll

# Test outputs
test_results/
*.log
*.trace

# Data directories (for local testing)
data/
logs/
*.data

# Docker volumes
volumes/

# Temporary files
tmp/
temp/
*.tmp

# Dependencies
third_party/
vendor/

# Generated files
*.pb.cc
*.pb.h
*.grpc.pb.cc
*.grpc.pb.h
!cedar_*.pb.cc
!cedar_*.pb.h
!meta_*.pb.cc
!meta_*.pb.h
!raft*.pb.cc
!raft*.pb.h

# Environment files
.env
.env.local

# Coverage
coverage/
*.gcov
*.gcda
*.gcno
EOF
echo "✅ .gitignore 已创建"

# 添加所有文件
echo ""
echo "📤 步骤 4: 添加文件到 Git"
git add -A
echo "✅ 文件已添加"

# 创建初始提交
echo ""
echo "💾 步骤 5: 创建初始提交"
if git rev-parse --verify HEAD >/dev/null 2>&1; then
    echo "✅ 提交已存在，跳过"
else
    git commit -m "🌲 Initial commit: CedarGraph distributed graph database

Core features:
- Distributed storage with Raft consensus
- LSM-tree based storage engine
- Temporal graph support
- gRPC connection pool
- Service auto-discovery
- Docker Compose deployment

See README.md for quick start guide."
    echo "✅ 初始提交已创建"
fi

# 创建 GitHub 仓库
echo ""
echo "🚀 步骤 6: 创建 GitHub 仓库"

if command -v gh &> /dev/null; then
    # 使用 GitHub CLI
    if gh repo view "$GITHUB_USER/CedarGraph" &> /dev/null; then
        echo "⚠️  仓库 $GITHUB_USER/CedarGraph 已存在"
    else
        echo "创建公开仓库..."
        gh repo create "$GITHUB_USER/CedarGraph" --public --source=. --remote=origin --push
        echo "✅ GitHub 仓库已创建并推送"
    fi
else
    # 手动指导
    echo ""
    echo "请手动在 GitHub 创建仓库:"
    echo "1. 访问: https://github.com/new"
    echo "2. 仓库名: CedarGraph"
    echo "3. 选择: Public (公开)"
    echo "4. 不要勾选 'Initialize this repository with a README'"
    echo "5. 点击 'Create repository'"
    echo ""
    echo "然后运行以下命令:"
    echo ""
    echo "  git remote add origin https://github.com/$GITHUB_USER/CedarGraph.git"
    echo "  git branch -M main"
    echo "  git push -u origin main"
    echo ""
fi

# 创建 cedar-docker-compose 仓库
echo ""
echo "🐳 步骤 7: 创建 cedar-docker-compose 仓库"

if [ -d "cedar-docker-compose" ]; then
    cd cedar-docker-compose
    
    if [ ! -d ".git" ]; then
        git init
        git add -A
        git commit -m "Initial commit: CedarGraph Docker Compose deployment

Features:
- One-click deployment with quick-start.sh
- Auto-discovery of storage nodes
- Pre-built Docker images
- CLI client and admin tools
- Kubernetes Helm Chart"
    fi
    
    if command -v gh &> /dev/null; then
        if gh repo view "$GITHUB_USER/cedar-docker-compose" &> /dev/null; then
            echo "⚠️  仓库 $GITHUB_USER/cedar-docker-compose 已存在"
        else
            gh repo create "$GITHUB_USER/cedar-docker-compose" --public --source=. --remote=origin --push
            echo "✅ cedar-docker-compose 仓库已创建"
        fi
    else
        echo ""
        echo "请手动创建 cedar-docker-compose 仓库:"
        echo "  https://github.com/new"
        echo ""
        echo "然后运行:"
        echo "  cd cedar-docker-compose"
        echo "  git remote add origin https://github.com/$GITHUB_USER/cedar-docker-compose.git"
        echo "  git branch -M main"
        echo "  git push -u origin main"
    fi
    
    cd ..
fi

echo ""
echo "================================"
echo "🎉 仓库初始化完成!"
echo ""
echo "GitHub 仓库地址:"
echo "  - https://github.com/$GITHUB_USER/CedarGraph"
echo "  - https://github.com/$GITHUB_USER/cedar-docker-compose"
echo ""
echo "下一步:"
echo "1. 设置 Docker Hub 自动发布 (见 .github/workflows/docker-release.yml)"
echo "2. 配置 GitHub Secrets: DOCKERHUB_USERNAME, DOCKERHUB_TOKEN"
echo "3. 在 README 中添加 CI/CD badge"
echo ""
