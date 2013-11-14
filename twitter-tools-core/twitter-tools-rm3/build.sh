#!/bin/sh
mvn clean package appassembler:assemble
rm target/appassembler/bin/*bat
chmod +x ./target/appassembler/bin/*
