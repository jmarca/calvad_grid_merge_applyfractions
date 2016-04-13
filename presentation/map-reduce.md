% Map-reduce?
% James E. Marca
% 2015-03-19

# Map reduce

1. collect data
2. process data

# Requirement

* the "reduce" step must be idempotent
* same input, same output
* no external calls


# Easy to parallelize

* multiple data sources
* no matter the source, same output
* works well with clusters of machines

# CouchDB Wrinkles

* just one machine
* "view" documents hold the reduce
* in theory, can use arbitrary javascript
* (but only rely on the document)
* (and using JS is incredibly slow in practice)

# CouchDB

* opportunity to learn Erlang!
* or just cache the view output
* run it once, save into another CouchDB
