#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# build.sh  —  编译 accel-proxy 全部组件
#
# 用法:
#   ./build.sh                  # 编译当前平台
#   ./build.sh --cross          # 交叉编译 linux/amd64  linux/arm64  darwin/amd64  darwin/arm64
#   ./build.sh --clean          # 仅清理 output 目录
#   GOOS=linux GOARCH=arm64 ./build.sh  # 手动指定目标平台
# ─────────────────────────────────────────────────────────────────────────────

# All binaries to build: name:cmd_path
BINARIES=(
    "accel-proxy:./cmd/proxy"
    "mux-server:./cmd/mux-server"
    "mux-client:./cmd/mux-client"
)

OUTPUT_DIR="$(mktemp -d ./output.XXXXXX)"

VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_VERSION=$(go version | awk '{print $3}')

LDFLAGS="-s -w \
  -X main.version=${VERSION} \
  -X main.buildTime=${BUILD_TIME} \
  -X main.goVersion=${GO_VERSION}"

# ── 颜色输出 ─────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[build]${NC} $*"; }
warn()  { echo -e "${YELLOW}[warn] ${NC} $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; }

# ── 清理函数 ─────────────────────────────────────────────────────────────────
cleanup_old_outputs() {
    local dirs
    dirs=$(ls -dt ./output.* 2>/dev/null || true)
    local count=0
    while IFS= read -r dir; do
        (( count++ )) || true
        if (( count > 3 )); then
            info "removing old output: $dir"
            rm -rf "$dir"
        fi
    done <<< "$dirs"
}

clean_all() {
    info "cleaning all output.* directories"
    rm -rf ./output.*
    info "done"
    exit 0
}

# ── 参数解析 ─────────────────────────────────────────────────────────────────
CROSS=false
for arg in "$@"; do
    case "$arg" in
        --clean) clean_all ;;
        --cross) CROSS=true ;;
        *) error "unknown argument: $arg"; exit 1 ;;
    esac
done

# ── 前置检查 ─────────────────────────────────────────────────────────────────
if ! command -v go &>/dev/null; then
    error "go not found in PATH"
    exit 1
fi

info "version    : ${VERSION}"
info "build time : ${BUILD_TIME}"
info "go         : ${GO_VERSION}"
info "output dir : ${OUTPUT_DIR}"

# ── 编译 ─────────────────────────────────────────────────────────────────────
build_one() {
    local binary="$1" cmd_path="$2" os="$3" arch="$4"
    local out="${OUTPUT_DIR}/${binary}-${os}-${arch}"
    [[ "$os" == "windows" ]] && out="${out}.exe"

    info "building ${binary} (${os}/${arch}) → ${out}"
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" \
        go build -trimpath -ldflags "${LDFLAGS}" -o "${out}" "${cmd_path}"
}

build_all() {
    local os="$1" arch="$2"
    for entry in "${BINARIES[@]}"; do
        IFS=':' read -r name cmd_path <<< "$entry"
        build_one "$name" "$cmd_path" "$os" "$arch"
    done
}

if $CROSS; then
    build_all linux  amd64
    build_all linux  arm64
    build_all darwin amd64
    build_all darwin arm64
else
    HOST_OS=$(go env GOOS)
    HOST_ARCH=$(go env GOARCH)
    build_all "${GOOS:-$HOST_OS}" "${GOARCH:-$HOST_ARCH}"
fi

# ── 写入构建信息 ──────────────────────────────────────────────────────────────
INFO_FILE="${OUTPUT_DIR}/build-info.txt"
{
    echo "version    : ${VERSION}"
    echo "build_time : ${BUILD_TIME}"
    echo "go_version : ${GO_VERSION}"
    echo "files:"
    ls -lh "${OUTPUT_DIR}/" | tail -n +2
} > "${INFO_FILE}"

info "build-info : ${INFO_FILE}"

# ── 清理旧输出目录 ────────────────────────────────────────────────────────────
cleanup_old_outputs

echo ""
info "✓ build complete → ${OUTPUT_DIR}/"
