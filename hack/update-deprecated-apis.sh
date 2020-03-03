#!/bin/sh

find vendor/github.com/libopenstorage/autopilot-api -name '*.go' -exec sed -i 's/DirectCodecFactory/WithoutConversionCodecFactory/g' {} \;

