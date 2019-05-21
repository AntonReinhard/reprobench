# reprobench

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/8163f0d20e9145cf9379a56f0383287c)](https://app.codacy.com/app/rkkautsar/reprobench?utm_source=github.com&utm_medium=referral&utm_content=rkkautsar/reprobench&utm_campaign=Badge_Grade_Dashboard)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/reprobench.svg)](https://pypi.org/project/reprobench)
[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=rkkautsar/reprobench)](https://dependabot.com)

## Development Guide

### Installing locally

Assuming a virtualenv has already been set up:

```sh
(env) $ pip install poetry
(env) $ poetry install -E all
(env) $ reprobench --help
```

This will install the required dependencies and install reprobench cli to the virtualenv. It will also be updated automatically when the source changes.
