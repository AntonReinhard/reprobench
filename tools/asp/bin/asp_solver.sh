#!/usr/bin/env bash

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our own variables:
verbose=0

while getopts "h?vt:s:f:" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    v)  verbose=1
        ;;
    t)  thp=1
        ;;
    s)  solver=$OPTARG
        ;;
    f)  filename=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

function interrupted(){
  kill -TERM $PID
  echo "Received signal..."
}
trap interrupted TERM
trap interrupted INT


if [ -z $solver ] ; then
  echo "No Solver given. Exiting..."
  exit 1
fi

if [ -z $filename ] ; then
  echo "No filename given. Exiting..."
  exit 1
fi

if [ ! -f $filename ] ; then
  echo "Filename does not exist. Exiting..."
  exit 1
fi

if [ ! -z $thp ] ; then
  env=GLIBC_THP_ALWAYS=1
  echo "Using THP option in libc"
fi

cd "$(dirname "$0")"
#get basic info
source ../../bash_shared/sysinfo.sh

solver_cmd="./$solver"_glibc $@

echo "$env $solver_cmd $filename"
echo
echo

#run call in background and wait for finishing
cat $filename | env $env $solver_cmd &
PID=$!
wait $PID
exit $?
