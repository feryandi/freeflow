#!/bin/bash

## Exit on error
set -o errexit
set -o errtrace

. read_ini.sh

## Constants
CONFIG=$AIRFLOW_HOME/conf/default.cfg

## Commands

CMD_0=("help" "" "Shows available commands" "help")
CMD_1=("deploy" "" "Shows available commands" "deploy_local")

COMMANDS=(
  CMD_0[@]
  CMD_1[@]
)
CMD_COUNT=${#COMMANDS[@]}

run() {
  [[ $# = 0 ]] && help && exit

  for ((i=0; i<$CMD_COUNT; i++)); do
    cmd="${!COMMANDS[i]:0:1}"
    params="${!COMMANDS[i]:1:1}"
    fun="${!COMMANDS[i]:3:1}"

    if [[ "${1}" = "${cmd}" ]]; then
      if [ -n "$(type -t ${fun})" ] && [ "$(type -t ${fun})" = function ]; then
        _temp_params=${params//[^\[]}
        num_params=${#_temp_params}

        [[ "${num_params}" > "$((${#@} - 1))" ]] && error "Missing parameters. Expected '${cmd} ${params}'" && exit 1
        "${fun}" ${@:2}
        exit

      else
        error "Command '${cmd}' not yet implemented"
        exit 1
      fi
    fi

  done
  error "Command '${1}' not found"
  exit 1
}


#### Writer Functions

log() { builtin echo -e [$(date -u +"%Y-%m-%d %H:%M:%S UTC")] ${@}; }
error() { log "\\x1b[31m${@}\\x1b[0m"; }
success() { log "\\x1b[32m${@}\\x1b[0m"; }
silently() { $1 &>/dev/null; }


## Helper

clean_process() {
  for pid in "${PIDS[@]}"; do
    kill -0 "$pid" && kill "$pid"
    success "Killed $pid"
  done
}


## Functions

help() {
  echo "Usage: ./$(basename "$0") [command]"
  for ((i=0; i<$CMD_COUNT; i++))
  do
    cmd=${!COMMANDS[i]:0:1}
    parameters=${!COMMANDS[i]:1:1}
    description=${!COMMANDS[i]:2:1}
    printf "  %-40s ${description}\n" "$cmd ${parameters}"
  done
}

deploy_local() {
  exit 1
}

main() {
  CONFIG=$AIRFLOW_HOME/conf/"${1}".cfg
  echo "${CONFIG}"
  read_ini "${CONFIG}"

  echo "${INI__deploy_type}"
  run "${@:2}"
}

main "${@}"
