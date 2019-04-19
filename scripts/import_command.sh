#!/bin/bash

neo4j-admin import --nodes=/import/neurons.csv --nodes=/import/synapses.csv --nodes=/import/connection_sets.csv --relationships=/import/_connects_to.csv --relationships=/import/_from.csv --relationships=/import/_to.csv --relationships=/import/_contains.csv

