from reprobench.core.server import BenchmarkServer
from loguru import logger
import sys
import yaml

#TODO: merge this into a single file

mconfig = None
with open('./benchmark_system_config.yml') as config_f:
    try:
        mconfig=yaml.safe_load(config_f)
    except yaml.YAMLError as exc:
        print(exc)
        exit(1)


# print(mconfig['server_url'])
# logger.add(sys.stdout, level="TRACE")
s = BenchmarkServer(mconfig['server_url'], verbosity=2)

s.run()
