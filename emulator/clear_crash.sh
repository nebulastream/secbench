#!/bin/bash

docker rm -f $(docker ps --filter 'label=com.containernet' -a -q)
mn -c
