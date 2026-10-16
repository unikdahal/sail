#!/usr/bin/env bash
# Runs a local Apache Celeborn cluster (one master and two workers) on 127.0.0.1
# without Docker, for Celeborn-backed integration tests and benchmarks.
#
# Usage:
#   scripts/celeborn/local-cluster.sh download   # fetch the Celeborn binary distribution
#   scripts/celeborn/local-cluster.sh start
#   scripts/celeborn/local-cluster.sh env        # print the variables for the pysail test fixtures
#   scripts/celeborn/local-cluster.sh stop
#
# Environment variables:
#   CELEBORN_VERSION  (default: 0.6.3)
#   CELEBORN_WORK_DIR (default: target/celeborn)
set -euo pipefail

version="${CELEBORN_VERSION:-0.6.3}"
work_dir="$(realpath -m "${CELEBORN_WORK_DIR:-target/celeborn}")"
home="${work_dir}/apache-celeborn-${version}-bin"
host=127.0.0.1
master_port=12097
master_http_port=12098
# rpc, push, fetch, replicate and HTTP ports of each worker
worker_ports=("12000 12001 12002 12003 12096" "12010 12011 12012 12013 12095")

download() {
  mkdir -p "${work_dir}"
  if [[ ! -d "${home}" ]]; then
    curl -sSfL --retry 5 -o "${work_dir}/celeborn.tgz" \
      "https://archive.apache.org/dist/celeborn/celeborn-${version}/apache-celeborn-${version}-bin.tgz"
    tar -xzf "${work_dir}/celeborn.tgz" -C "${work_dir}"
    rm "${work_dir}/celeborn.tgz"
  fi
}

write_config() {
  local dir="$1"
  mkdir -p "${dir}/conf" "${dir}/logs" "${dir}/pid" "${dir}/data"
  cat > "${dir}/conf/celeborn-defaults.conf" <<CONF
celeborn.master.ha.enabled false
celeborn.master.endpoints ${host}:${master_port}
celeborn.master.host ${host}
celeborn.master.port ${master_port}
celeborn.master.http.host ${host}
celeborn.master.http.port ${master_http_port}
celeborn.worker.storage.disk.reserve.size 1m
CONF
}

run() {
  local dir="$1"
  shift
  CELEBORN_CONF_DIR="${dir}/conf" CELEBORN_LOG_DIR="${dir}/logs" CELEBORN_PID_DIR="${dir}/pid" \
    CELEBORN_LOCAL_HOSTNAME="${host}" CELEBORN_MASTER_MEMORY=512m CELEBORN_WORKER_MEMORY=512m \
    CELEBORN_WORKER_OFFHEAP_MEMORY=512m JAVA_TOOL_OPTIONS='' "$@"
}

wait_for() {
  local description="$1"
  shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for ${description}" >&2
  exit 1
}

registered_workers() {
  curl -sf "http://${host}:${master_http_port}/api/v1/workers" | grep -o '"rpcPort"' | wc -l
}

workers_ready() {
  [[ "$(registered_workers)" -ge "${#worker_ports[@]}" ]]
}

start() {
  download
  local master="${work_dir}/run/master"
  write_config "${master}"
  run "${master}" "${home}/sbin/start-master.sh" --host "${host}" --port "${master_port}"
  wait_for "the Celeborn master" curl -sf "http://${host}:${master_http_port}/api/v1/workers"
  local i=0
  for ports in "${worker_ports[@]}"; do
    read -r rpc push fetch replicate http <<<"${ports}"
    i=$((i + 1))
    local dir="${work_dir}/run/worker-${i}"
    write_config "${dir}"
    cat >> "${dir}/conf/celeborn-defaults.conf" <<CONF
celeborn.worker.rpc.port ${rpc}
celeborn.worker.push.port ${push}
celeborn.worker.fetch.port ${fetch}
celeborn.worker.replicate.port ${replicate}
celeborn.worker.http.host ${host}
celeborn.worker.http.port ${http}
celeborn.worker.storage.dirs ${dir}/data:capacity=4g:disktype=HDD:flushthread=1
CONF
    run "${dir}" "${home}/sbin/start-worker.sh" --host "${host}" --port "${rpc}" \
      "celeborn://${host}:${master_port}"
  done
  wait_for "the Celeborn workers" workers_ready
  env_vars
}

stop() {
  for dir in "${work_dir}"/run/worker-* "${work_dir}/run/master"; do
    [[ -d "${dir}" ]] || continue
    if [[ "${dir}" == *master ]]; then
      run "${dir}" "${home}/sbin/stop-master.sh" || true
    else
      run "${dir}" "${home}/sbin/stop-worker.sh" || true
    fi
  done
}

env_vars() {
  local workers=()
  for ports in "${worker_ports[@]}"; do
    read -r rpc push fetch replicate _ <<<"${ports}"
    workers+=("${host}:${rpc}:${push}:${fetch}:${replicate}")
  done
  echo "export SAIL_TEST_CELEBORN_MASTER=${host}:${master_port}:${master_http_port}"
  echo "export SAIL_TEST_CELEBORN_WORKERS=$(IFS=,; echo "${workers[*]}")"
}

case "${1:-}" in
  download) download ;;
  start) start ;;
  stop) stop ;;
  env) env_vars ;;
  *)
    echo "usage: $0 {download|start|stop|env}" >&2
    exit 2
    ;;
esac
