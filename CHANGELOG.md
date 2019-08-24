# Changelog

## 3.1.0

* Error objects will now return query metadata, including parameters. In production
environments, it is recommended that the parameter values be excluded from logs.

## 3.0.0

New features:

* `localDataCenter` is now a required field in `cassandra-driver` version 4.

## 2.2.0

New features:

* Support a `localDataCenter` config setting to use in the load balancing policy
