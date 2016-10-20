#!/bin/bash

# go get -v github.com/wvanbergen/kafka/tools/stressproducer
stressproducer -verbose 2>&1 | head -n100
