# ttlcache

ttlcache is a distributed cache heavily influenced by [groupcache](https://github.com/golang/groupcache).
The primary difference is ttlcache supports indication of a TTL, and will lazily refetch a value when its TTL expires.
It was created to avoid thundering herds when a key's value is not yet known.
An early incarnation involved an embargo period on failed loads, however it turned out to be both more useful and easier to implement to have a generic TTL on every key, and return a special value to indicate a lack thereof.
All the usual groupcache conveniences are maintained.

 - Per-key time-to-lives, specified when the value is provided.
 - Only [memberlist](https://github.com/hashicorp/memberlist) is supported for cluster membership.
 - All keys are `string`, and all values are `[]byte`. We assume values are only useful in their entirety.
 - Native [Prometheus](https://prometheus.io/) metrics following conventions.
 - Dynamic reloading of the key retriever, peer loader, and TTL override list.
