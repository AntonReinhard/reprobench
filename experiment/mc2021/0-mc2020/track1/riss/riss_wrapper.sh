#!/usr/bin/env bash
#set -x
# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

echo "c o CALLSTR $@"

for i in "$@"
do
case $i in
    -t=*|--tmpdir=*)
    PTMPDIR="${i#*=}"
    shift # past argument=value
    ;;
    -r=*|--maxrss=*)
    PMAXRSS="${i#*=}"
    shift # past argument=value
    ;;
    -d=*|--maxtmp=*)
    PMAXTMP="${i#*=}"
    shift # past argument=value
    ;;
    -w=*|--timeout=*)
    PTIMEOUT="${i#*=}"
    shift # past argument=value
    ;;
    -a=*|--task=*)
    PTASK="${i#*=}"
    shift # past argument=value
    ;;
    --default)
    DEFAULT=YES
    shift # past argument with no value
    ;;
    *)
          # unknown option
    FILE=$i
    ;;
esac
done
shift $((OPTIND))

echo "c o =============== TEST CMDLINE ARGS ========================="
echo "c o CMDLINE TMPDIR = ${PTMPDIR}"
echo "c o CMDLINE PMAXTMP = ${PMAXTMP}"
echo "c o CMDLINE MAXRSS = ${PMAXRSS}"
echo "c o CMDLINE TIMEOUT = ${PTIMEOUT}"

echo "c o =============== TEST ENV VARS ==========================="
echo "c o ENV TMPDIR = ${TMPDIR}"
echo "c o ENV PMAXTMP = ${MAXTMP}"
echo "c o ENV MAXRSS = ${MAXRSS}"
echo "c o ENV TIMEOUT = ${TIMEOUT}"

echo "c o ================= SET PRIM INTRT HANDLING ==============="
function interrupted(){
  echo "c o Sending kill to subprocess"
  kill -TERM $PID
  echo "c o Removing tmp files"
  [ ! -z "$prec_tmpfile" ] && rm prec_tmpfile
  [ ! -z "$tmpfile" ] && rm $tmpfile
}

function finish {
  # Your cleanup code here
  echo "c o Removing tmp files"
  [ ! -z "$prec_tmpfile" ] && rm prec_tmpfile
  [ ! -z "$tmpfile" ] && rm $tmpfile
}
trap finish EXIT
trap interrupted TERM
trap interrupted INT

echo "c o ================= POS CMDLINE ARGS ==============="
echo "c o $*"

echo "c o ================= Changing directory to output directory ==============="
cd "$(dirname "$TMPDIR")" || (echo "Could not change directory to $TMPDIR. Exiting..."; exit 1)

BIN_DIR=$(realpath $(dirname $0))

tmpfile=$(mktemp ${PTMPDIR}/result.XXXXXX)

echo "c o ================= RUNNING COPROCESSOR ONLY ==============="
cmd="$BIN_DIR/scripts/coprocessor-for-modelcounting.sh $FILE"
myenv="TMPDIR=$TMPDIR"
echo "c o SOLVERCMD=$cmd"

$cmd > $tmpfile &

PID=$!
wait $PID
exit_code=$?
echo "c solver_wrapper: ==============================="
echo "c solver_wrapper: Solver finished with exit code="$exit_code
echo "c f RET="$exit_code

cat $tmpfile

if ! [ $exit_code -eq "0" ] ; then
  echo "s UNKNOWN"
  exit 1
fi

result=$(cat $tmpfile | grep "^s " | awk '{print $3}')

if [ -z "$result" ] ; then
  echo "s UNKNOWN"
  exit 1
fi

if [ $result -eq "0" ] ; then
  echo "s UNSATISFIABLE"
  echo "c s $PTASK"
  echo "c s log10-estimate inf"
  echo "c s exact quadruple int 0"
else
  echo "s SATISFIABLE"
  echo "c type $PTASK"
  #let's play codegolf
  log10=$(echo $result | python3 -c 'import sys; import math; print(math.log10(int(sys.stdin.readline())));')
  echo "c s log10-estimate $log10"
  echo "c s exact quadruple int $result"
fi

exit $exit_code
