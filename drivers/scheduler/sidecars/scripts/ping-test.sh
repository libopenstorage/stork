#!/usr/bin/env sh

while ! curl ${HOST} ; do sleep 5 ; done
