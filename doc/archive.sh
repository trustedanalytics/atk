#!/bin/bash

pushd build
  tar -pczf ../../docs.tar.gz html/*
popd