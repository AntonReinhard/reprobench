import atexit
import json
import math
import multiprocessing as mp
import os
import re
import subprocess
import sys
import threading
import time
import urllib
from functools import reduce
from pathlib import Path
from sys import platform
import socket as sct

import click
import zmq
from loguru import logger
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError

from reprobench.console.decorators import common, server_info, use_tunneling
from reprobench.core.events import (
    RUN_FINISH,
    RUN_INTERRUPT,
    RUN_START,
    RUN_STEP,
    WORKER_JOIN
)
from reprobench.utils import decode_message, import_class, send_event

REQUEST_TIMEOUT = 15000

class BenchmarkWorker:
    @staticmethod
    def cnt(values, lst):
        ret = 0
        for e in values:
            if e in lst.keys():
                ret += 1
        return ret

    def __init__(self, server_address, tunneling, multicore: dict = None, cluster_job_id=None):
        self.server_address = server_address

        # Handling is required for cluster submission
        logger.info(f"Multicore parameters are: {multicore}")
        if isinstance(multicore, str):
            multicore = json.loads(urllib.parse.unquote(multicore))
        if multicore is None:
            multicore = {}
        elif BenchmarkWorker.cnt(['processes', 'cores_per_process', 'pinning'], multicore) > 1:
            logger.error("Incompatible options. Configuration requires either processes or specific pinning.")
            raise ValueError('Invalid call.')

        self.pinning = multicore.get('pinning', None)
        self.processes = multicore.get('processes', None)
        self.cores_per_process = multicore.get('cores_per_process', None)
        if self.pinning is None and self.processes is None and self.cores_per_process is None:
            self.processes = 0
        # TODO: remove OS independence does not work anyways due to process handling etc
        self.isunix = (platform == "linux" or platform == "linux2")
        if cluster_job_id is None:
            self.cluster_job_id = int(os.getenv('SLURM_ARRAY_JOB_ID', -1))
        else:
            logger.warning("*" * 80)
            logger.warning(f"The cluster_job_id has been set manually to {cluster_job_id}... (jobs might not match)")
            logger.warning("*" * 80)
            self.cluster_job_id = cluster_job_id

        if tunneling is not None:
            for _ in range(1, 5):
                try:
                    self.server = SSHTunnelForwarder(
                        tunneling["host"],
                        remote_bind_address=("127.0.0.1", tunneling["port"]),
                        ssh_pkey=tunneling["key_file"],
                        ssh_config_file=tunneling["ssh_config_file"],
                    )

                    # https://github.com/pahaz/sshtunnel/issues/138
                    if sys.version_info[0] > 3 or (
                        sys.version_info[0] == 3 and sys.version_info[1] >= 7
                    ):
                        self.server.daemon_forward_servers = True

                    self.server.start()
                    self.server_address = f"tcp://127.0.0.1:{self.server.local_bind_port}"
                    logger.info(f"Tunneling established at {self.server_address}")
                    time.sleep(1)  # Let the tunnel establish
                    self.server.check_tunnels()

                    atexit.register(self.stop_tunneling)
                    return
                except (BaseSSHTunnelForwarderError, ConnectionResetError) as ee:
                    logger.info(f"Worker spawn failed {ee}")
                    time.sleep(3)
        else:
            self.server = None

    def killed(self, run_id):
        if self.socket:
            # TODO: Modify atexit events...
            send_event(self.socket, RUN_INTERRUPT, run_id)

    def stop_tunneling(self):
        self.server.stop()

    def run(self):
        # noinspection PyUnusedLocal
        def internal_runner(cworker_id, core_pinning: list):
            logger.debug(f"Starting worker #{cworker_id} for Cores {core_pinning}")
            ret = mp.Value("b", True, lock=False)

            while ret.value:
                ip = mp.Process(target=self.run_internal, args=[core_pinning, ret])
                ip.start()
                # A timeout here could enforce a runtime limit...
                ip.join()

            logger.debug(f"Worker for CPU #{cworker_id} for cores {core_pinning} finished")

        # Check if socket is connected, or reconnect on demand....
        if self.processes is not None and self.processes == 0:
            self.run_internal()
        else:
            pin_controller = {}
            # Pin this controlling thread to the first CPU (unix only)
            try:
                if self.pinning is not None and 'controller' in self.pinning:
                    pin_controller = set(self.pinning['controller'])
                else:
                    pin_controller = {0}
                logger.info(f"Controller affinity will be set to {pin_controller}")
                os.sched_setaffinity(0, pin_controller)
                res = os.sched_getaffinity(0)
                if res != pin_controller:
                    logger.error(f"Setting controller affinity was not successful. Is now: {res}")
            except OSError as ee:
                logger.error(f"Failed to set affinity for control thread {ee}")

            # Get number of physical CPUs, not cores
            val = subprocess.check_output('numactl --hardware', shell=True, text=True)
            regex_p = re.compile(r"\s*available:\s*(?P<pcore>[0-9]+)\s*nodes\s*\((?P<nodes>([0-9]+-[0-9]+|[0-9]+))\)")
            rcores = regex_p.match(val.split("\n")[0])
            if rcores:
                num_phys_cores, numa_nodes = rcores.group("pcore"), rcores.group("nodes")
            else:
                logger.error(f"Unexpected output from numactl. Result was:")
                for line in val.split("\n"):
                    logger.error(f"\t{line}")
                raise RuntimeError

            logger.info(f"Number of available cores: {num_phys_cores}")
            logger.info(f"Available nodes: {numa_nodes}")

            # Get number of cores on NUMA regions
            cpus = {}
            regex_c = re.compile(r"\s*node\s*(?P<node>[0-9]+)\s*cpus:(?P<cpus>(\s[0-9]+)*)\s*")
            for line in val.split("\n"):
                rncores = regex_c.match(line)
                if rncores:
                    cpus[int(rncores.group("node"))] = set(
                        map(lambda x: int(x) if x != '' else -1, rncores.group("cpus")[1:].split(' ')))
            logger.info(f"NUMA Cores: {cpus}")

            pinning = {}
            if self.processes is not None or self.cores_per_process is not None:
                remaining_cores = cpus
                for p_core in remaining_cores:
                    remaining_cores[p_core] = remaining_cores[p_core] - pin_controller

                logger.info(f"Remaining cores after pinning controller thread {remaining_cores}")
                if all(remaining_cores[pc] == {} for pc in remaining_cores):
                    logger.warning(f"No cores remaining for pinning... Setting to 0")
                    remaining_cores[0] = {0}

                available_cores = reduce(lambda x, y: x | y, remaining_cores.values())
                logger.debug(f"Following cores are available {available_cores}")
                if self.processes is not None:
                    cores_per_task = math.floor(len(available_cores) / self.processes)
                else:
                    cores_per_task = self.cores_per_process
                logger.debug(f"Using {cores_per_task} cores per task.")
                if cores_per_task < 1:
                    logger.error(
                        f"Current configuration is not stable. Less than one core per task left. "
                        f"(Available: {len(available_cores)} Expected: {self.processes})")
                    raise RuntimeError("Unstable configuration.")
                worker_id = 1
                for p_core in reversed(sorted(remaining_cores.keys())):
                    pcpu = list(reversed(sorted(remaining_cores[p_core])))
                    for f_core_id in range(len(pcpu))[::cores_per_task]:
                        if self.processes is not None and worker_id > self.processes:
                            break
                        if f_core_id + cores_per_task > len(pcpu):
                            break
                        assigning_cores = {pcpu[e] for e in range(f_core_id, f_core_id + cores_per_task)}
                        pinning[worker_id] = assigning_cores
                        worker_id += 1

                if self.processes is not None and worker_id < self.processes:
                    logger.error("Could not assign all workers to NUMA regions...")
                    logger.error(f"Will run with {worker_id} workers.")
                logger.debug(f"We use the following pinning for the workers {pinning}")
                if pinning == dict():
                    logger.error(f"Could not compute pinning with current configuration. Obtained an empty pinning for {cores_per_task} cores per task.")
                    logger.error(f"NUMA retured\n{val}")
                workers = list(pinning.keys())
            else:
                workers = list(map(int, self.pinning['worker'].keys()))
                for worker, core in self.pinning['worker'].items():
                    if isinstance(core, str) and '..' in core:
                        v = list(map(int, core.split('..')))
                        pcores = set(range(v[0], v[1] + 1))
                    else:
                        pcores = {int(core)}
                    pinning[int(worker)] = pcores
                    logger.info(f"Worker {worker} will pin to {pcores}")

                logger.error(f"Workers/Cores: {workers}")
                pinned_cores = {int(x) for v in pinning.values() for x in v}
                numa_cores = {int(x) for v in cpus.values() for x in v}
                if not pinned_cores <= numa_cores:
                    logger.error(
                        f"Worker pinning {pinning} [used cores: {pinned_cores}] does not agree with hardware core "
                        f"regions obtained from NUMActl {cpus} [expected max cores: {numa_cores}].")
                    logger.error(f"Disabling core pinning and falling back to one worker.")
                    workers = [1]
                    pinning = {1: None}

            runner_threads = []
            logger.debug(f"Using the following workers: {workers}")
            for worker_id in workers:
                logger.debug(f"Running {worker_id}")
                t = threading.Thread(target=internal_runner, args=[worker_id, pinning[worker_id]])
                runner_threads.append(t)
                t.start()
                # Wait to avoid parallel connections
                time.sleep(0.2)

            for t in runner_threads:
                try:
                    t.join()
                # If the thread is finished already, join causes a runtime exception,
                #   so this is expected behaviour
                # Checking for an active thread before would create a race condition,
                #   is the thread could finish after the check, before the join.
                except RuntimeError:
                    pass
        if self.server is not None:
            self.stop_tunneling()

    def run_internal(self, core_pinning: list = None, ret=None):
        # Pin process, this affects subprocesses as well
        if self.isunix and core_pinning is not None:
            try:
                os.sched_setaffinity(0, core_pinning)
                logger.debug(f"Pinned to core {core_pinning}")
            except OSError as ee:
                logger.error(f"Failed to set affinity {ee}")

        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        logger.debug(f"Connecting to {self.server_address}")
        socket.connect(self.server_address)
        logger.debug(f"Connected to {self.server_address}")

        # TODO: might need update for SGE
        send_event(socket, WORKER_JOIN, dict(cluster_job_id=self.cluster_job_id, hostname=sct.gethostname()))

        run = decode_message(socket.recv())
        logger.trace(f"Run to {run}")
        if run is None:
            logger.info("No more tasks left, exiting...")
            if ret is not None:
                ret.value = False
            return False

        logger.info(f"Running {run['task']} with tool {run['tool']}({run['tool_name']}) and "
                    f"parameters {run['parameters']} limits: {run['limits']}")

        run_id = run["id"]
        # TODO: This probably needs correction
        atexit.register(self.killed, run_id)

        tool = import_class(run["tool"])

        if not tool.is_ready():
            tool.setup()

        context = {"socket": socket, "tool": tool, "tool_name": run["tool_name"], "run": run,
                   "server_address": self.server_address}
        logger.info(f"Processing task: {run['id']}")

        directory = Path(run["id"])
        directory.mkdir(parents=True, exist_ok=True)

        payload = dict(tool_version=tool.version(), run_id=run_id)
        send_event(socket=socket, event_type=RUN_START, payload=payload, disconnect=True)

        for runstep in run["steps"]:
            logger.debug(f"Running step {runstep['module']}")
            step = import_class(runstep["module"])
            config = json.loads(runstep["config"])
            step.execute(context, config)
            payload = {"run_id": run_id, "step": runstep["module"]}
            send_event(socket=socket, event_type=RUN_STEP, payload=payload,
                       reconnect=self.server_address, disconnect=True)

        logger.debug(f"Reporting finished.")
        send_event(socket=socket, event_type=RUN_FINISH, payload=run_id, reconnect=self.server_address)
        # TODO: Probably needs changing
        atexit.unregister(self.killed)

        socket.close()

        if ret is not None:
            ret.value = True
        return True


@click.command("worker")
@click.option("--multicore", type=str, default=None)
@server_info
@use_tunneling
@common
def cli(**kwargs):
    worker = BenchmarkWorker(**kwargs)
    worker.run()


if __name__ == "__main__":
    cli()
