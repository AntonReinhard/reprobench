# TODO: replace platform by standardlib
import json
import os
import pathlib
import platform
import re
import tempfile
from subprocess import Popen, PIPE

from loguru import logger

import reprobench
from reprobench.utils import send_event
from .base import Executor
from .db import RunStatisticExtended
from .events import STORE_THP_RUNSTATS

runsolver_re = {
    "SEGFAULT": re.compile(r"^\s*Child\s*ended\s*because\s*it\s*received\s*signal\s*11\s*\((?P<val>SIGSEGV)\)\s*"),
    "STATUS": re.compile(r"Child status: (?P<val>[0-9]+)"),
}

perf_re = {
    "dTLB_load_misses": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*dTLB-load-misses\s*"),
    "dTLB_loads": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*dTLB-loads\s*"),
    "dTLB_store_misses": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*dTLB-store-misses\s*"),
    "dTLB_stores": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*dTLB-stores\s*"),
    "iTLB_load_misses": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*iTLB-load-misses\s*"),
    "iTLB_loads": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*iTLB-loads\s*"),
    "cycles": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*cycles\s*"),
    "stall_cycles": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*stalled-cycles-backend\s*"),
    "cache_misses": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*cache-misses\s*"),
    "elapsed": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*seconds time elapsed\s*"),
    "cpu_migrations": re.compile(r"\s*(?P<val>[0-9]+(\.[0-9]+)?)\s*cpu-migrations\s*"),
    "page_faults": re.compile(r"\s*(?P<val>[0-9]+)\s*page-faults\s*"),
    "context_switches": re.compile(r"\s*context-switches\s*"),
}


class RunSolverPerfEval(Executor):
    def __init__(self, context, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.socket = context["socket"]
        self.server_address = context["server_address"]
        self.run_id = context["run"]["id"]

        if config is None:
            config = {}

        wall_grace = config.get("wall_grace", 15)
        self.nonzero_as_rte = config.get("nonzero_rte", True)

        limits = context["run"]["limits"]
        time_limit = float(limits["time"])
        megabytes = 1

        self.wall_limit = time_limit + wall_grace
        self.cpu_limit = time_limit
        self.mem_limit = float(limits["memory"]) * megabytes
        self.reprobench_path = os.path.abspath(os.path.join(os.path.dirname(reprobench.__file__), '..'))

    @classmethod
    def register(cls, config=None):
        RunStatisticExtended.create_table()

    @staticmethod
    def compile_stats(stats, run_id, nonzero_as_rte):
        perf_keys = ['perf_dTLB_load_misses', 'perf_dTLB_loads', 'perf_dTLB_store_misses',
                     'perf_dTLB_stores', 'perf_iTLB_load_misses', 'perf_iTLB_loads',
                     'perf_cycles', 'perf_cache_misses', 'perf_elapsed']

        if 'runsolver_error' in stats:
            stats['cpu_time'] = '-1'
            stats['wall_time'] = '-1'
            stats['max_memory'] = '-1'
            stats['return_code'] = '-1'
            stats['verdict'] = RunStatisticExtended.RUNTIME_ERR
        else:
            try:
                stats['return_code'] = stats['runsolver_STATUS']
            except KeyError:
                stats['return_code'] = '9'
            stats['cpu_time'] = stats['runsolver_CPUTIME']
            stats['wall_time'] = stats['runsolver_WCTIME']
            stats['max_memory'] = stats['runsolver_MAXVM']

            if stats["runsolver_TIMEOUT"] == 'true':
                verdict = RunStatisticExtended.TIMEOUT
            elif stats["runsolver_MEMOUT"] == 'true':
                verdict = RunStatisticExtended.MEMOUT
            elif ("error" in stats and stats["error"] != '') or (
                    nonzero_as_rte and nonzero_as_rte.lower() == 'true' and int(stats['return_code']) != 0):
                verdict = RunStatisticExtended.RUNTIME_ERR
            else:
                verdict = RunStatisticExtended.SUCCESS
            if 'runsolver_STATUS' not in stats:
                stats['runsolver_STATUS'] = 1

            stats['verdict'] = verdict

        if 'error' in stats:
            del stats["error"]

        for key in perf_keys:
            if key not in stats:
                stats[key] = '-1'

        stats['run_id'] = run_id

        logger.trace(stats)
        return stats

    @staticmethod
    def exec_lib(path, stdout, stderr, cwd=str(pathlib.Path.cwd())):
        output = None
        if isinstance(path, pathlib.Path):
            path = str(path)
        if os.path.exists(path):
            logger.trace(f"File for sysinfo found at {path}")
            if not os.access(path, os.X_OK):
                logger.error(f"File for sysinfo found at {path}. But is NOT executable!!")
            else:
                p_stats = Popen(path, stdout=PIPE, stderr=PIPE, shell=True,
                                close_fds=True, cwd=cwd)
                output, err = p_stats.communicate()
                logger.trace(output)
                with open(stdout, 'w+') as f:
                    f.write(str(output))
                if len(err) != 0:
                    logger.error(err)
                    with open(stderr, 'w+') as f:
                        f.write(str(err))
        else:
            logger.error(f"Expected file for sysinfo at {path}, but was missing.")
        return output

    def prerun(self, cmdline, out_path, **kwargs):
        outdir = self.output_dir(out_path)
        stdout_p, stderr_p, _, _, _, _, _ = self.log_paths(outdir)

        binary = "lib/sysinfo.sh"
        sysinfo_path = pathlib.Path(cmdline[0]).parent.parent.parent.joinpath(binary).resolve()
        RunSolverPerfEval.exec_lib(path=sysinfo_path, stdout=stdout_p, stderr=stderr_p)
        RunSolverPerfEval.exec_lib(path=pathlib.Path.cwd().joinpath("lib/sysinfo.sh").absolute(), stdout=stdout_p,
                                   stderr=stderr_p)

    def run(
        self,
        cmdline,
        out_path=None,
        err_path=None,
        input_str=None,
        directory=None,
        **kwargs,
    ):
        stats = {'platform': platform.platform(aliased=True), 'hostname': platform.node()}

        cmdline[0] = str(pathlib.Path(cmdline[0]).resolve())

        with tempfile.NamedTemporaryFile(prefix='rsolve_perf_tmp', dir='/dev/shm', delete=True) as f:
            logger.debug(f"Extracting instance {input_str} to {f.name}")
            transparent_cat = f"{self.reprobench_path}/lib/tcat.sh {input_str} -o {f.name}"

            logger.trace(transparent_cat)
            p_tmpout = Popen(transparent_cat, stdout=PIPE, stderr=PIPE, shell=True, close_fds=True,
                             cwd=self.reprobench_path)
            output, err = p_tmpout.communicate()
            if err != b'':
                logger.error(err)
                stats['error'] = err
                exit(1)
            else:
                logger.debug(f"Instance is available at {f.name}")

            # TODO: fix out_path
            outdir = self.output_dir(out_path)
            payload_p, perflog, stderr_p, stdout_p, varfile, watcher, runparameters_p = self.log_paths(outdir)

            logger.debug(perflog)

            ifilename = input_str
            if "-f" in ifilename:
                ifilename = ifilename.split(" ")[1]
            solver_cmd = f"{' '.join(cmdline)} -f {f.name} -i {ifilename}"
            logger.trace(f"Solver command was: {solver_cmd}")
            # perf list
            perfcmdline = f"/usr/bin/perf stat -o {perflog} -e dTLB-load-misses,dTLB-loads,dTLB-store-misses," \
                          f"dTLB-stores,iTLB-load-misses,iTLB-loads,cycles,stalled-cycles-backend,cache-misses " \
                          f"{solver_cmd}"
            runsolver = os.path.expanduser("~/bin/runsolver")
            run_cmd = f"{runsolver:s} --vsize-limit {self.mem_limit:.0f} -W {self.cpu_limit:.0f}  -w {watcher:s} " \
                      f"-v {varfile:s} {perfcmdline:s} > {stdout_p:s} 2>> {stderr_p:s}"

            logger.trace(f'Logging run parameters to {runparameters_p}')
            with open(runparameters_p, 'w') as runparameters_f:
                run_details = {'run_id': self.run_id}
                runparameters_f.write(json.dumps(run_details))

            logger.trace(run_cmd)
            logger.info(f"Running {directory}")
            p_solver = Popen(run_cmd, stdout=PIPE, stderr=PIPE, shell=True, close_fds=True, cwd=outdir)
            output, err = p_solver.communicate()

            if err != b'':
                logger.error(err)
                stats['error'] = err
            else:
                stats['error'] = ''

            stats = self.parse_logs(perflog, varfile, watcher, stats)

            logger.trace(stats)
            logger.debug(f"Finished {directory}")

            payload = self.compile_stats(stats, self.run_id, self.nonzero_as_rte)

            logger.trace(f"payload: {payload}")
            with open(payload_p, 'w') as payload_f:
                payload_f.write(json.dumps(payload))

            # send_event(self.socket, STORE_RUNSTATS, payload)
            send_event(socket=self.socket, event_type=STORE_THP_RUNSTATS, payload=payload,
                       reconnect=self.server_address, disconnect=True)

    def output_dir(self, out_path):
        return os.path.abspath(os.path.join(self.reprobench_path, os.path.dirname(out_path)))

    @staticmethod
    def log_paths(outdir):
        stdout_p = f'{outdir}/stdout.txt'
        stderr_p = f'{outdir}/stderr.txt'
        watcher = f'{outdir}/watcher.txt'
        varfile = f'{outdir}/varfile.txt'
        perflog = f'{outdir}/perflog.txt'
        payload_p = f'{outdir}/result.json'
        runparameters_p = f'{outdir}/run.json'
        return payload_p, perflog, stderr_p, stdout_p, varfile, watcher, runparameters_p

    @staticmethod
    def parse_logs(perflog, varfile, watcher, stats=None):
        if stats is None:
            stats = {}
        try:
            # runsolver parser
            with open(f"{varfile:s}") as f:
                for line in f:
                    if line.startswith('#') or len(line) == 0:
                        continue
                    line = line[:-1].split("=")
                    stats[f'runsolver_{line[0]}'] = line[1]
            logger.trace(stats)
        except FileNotFoundError as e:
            logger.error(e)
            stats['runsolver_error'] = 'true'

        # runsolver watcher parser (returncode etc)
        # for line in codecs.open(perflog, errors='ignore', encoding='utf-8'):
        with open(f"{watcher:s}") as f:
            for line in f.readlines():
                for val, reg in runsolver_re.items():
                    m = reg.match(line)
                    if m:
                        stats[f'runsolver_{val}'] = m.group("val")
        logger.trace(stats)
        # perf result parser
        with open(f"{perflog:s}") as f:
            for line in f.readlines():
                # logger.debug(line)
                for val, reg in perf_re.items():
                    m = reg.match(line)
                    if m:
                        stats[f'perf_{val}'] = m.group("val")
        return stats
