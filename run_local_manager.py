import argparse
import os
import pathlib

import yaml
import sys

from reprobench.managers.local import LocalManager
from loguru import logger

logger.remove()

#TODO: move to click?
parser = argparse.ArgumentParser(description='%(prog)s -r remoteid:port -i cluster_job_id')
parser.add_argument('-i', '--cluster_job_id', dest='cluster_job_id', action='store',
                    default=-1, help='Set cluster_job_id [default=-1]')
args = parser.parse_args()

path = pathlib.Path(__file__).parent.resolve()
os.chdir(path)

mconfig = None
with open(os.path.join(path, './benchmark_system_config.yml')) as config_f:
    try:
        mconfig = yaml.safe_load(config_f)
    except yaml.YAMLError as exc:
        print(exc)
        exit(1)

m = LocalManager(server_address=mconfig['server_url'], output_dir=mconfig['output'], multicore=mconfig['multicore'],
                 config=mconfig['default_exp_config'], tunneling=None, repeat=mconfig['repeat'], rbdir="",
                 cluster_job_id=args.cluster_job_id)

m.run()
