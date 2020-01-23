#!/usr/bin/env bash

if [[ ! -f venv/bin/activate ]]; then
    pip install virtualenv --user
    virtualenv venv --no-site-packages --python=python3
fi

export PIP_REQUIRE_VIRTUALENV=true
export PIP_RESPECT_VIRTUALENV=true

source venv/bin/activate
pip install -U pip

pip install  -r  requirements.txt
