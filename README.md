# ttlcache

[![GoDoc](https://godoc.org/github.com/gebn/ttlcache?status.svg)](https://godoc.org/github.com/gebn/ttlcache)
[![Go Report Card](https://goreportcard.com/badge/github.com/gebn/ttlcache)](https://goreportcard.com/report/github.com/gebn/ttlcache)

ttlcache is a distributed cache heavily influenced by [groupcache](https://github.com/golang/groupcache).
The primary difference is ttlcache supports providing a TTL when loading a value, and will lazily refetch values when their TTL expires.
It was created to avoid thundering herds when a key's value is not yet known.
An early incarnation involved an embargo period on failed loads, however it turned out to be both more useful and easier to implement to have a generic TTL on every key, and return a special value to indicate a lack thereof.
All the usual groupcache conveniences are maintained.

 - Per-key time-to-lives, specified when the value is provided.
 - Only [memberlist](https://github.com/hashicorp/memberlist) is supported for cluster membership.
 - All keys are `string`, and all values are `[]byte`. We assume values are only useful in their entirety.
 - Native [Prometheus](https://prometheus.io/) metrics following conventions.
 - Dynamic reloading of the key retriever, peer loader, and TTL override list.

## Motivation

groupcache is excellent if we can guarantee that only keys that exist are requested.
This is enforceable for dl.google.com, however we wanted to be able to handle keys that gradually come into existence over time.
ttlcache assumes it is just as expensive to find out whether a value exists as it is to retrieve it.
It avoids thundering herds both when the key exists, but also when we are sure it does not (as opposed to a load error).
The only option in this situation with groupcache is to return an error, which causes a thundering herd of retries.
