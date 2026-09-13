# cache-backend-redis

RESP3 Redis / Valkey backend for [`cache-protocol`](https://github.com/egao1980/cache-protocol). Codec talks over a stream; `send-fn` is injectable. Unit tests do **not** need a live Valkey — they inject a fake RESP server (in-memory dict + codec).

```lisp
(asdf:load-system "cache-backend-redis")

(let* ((fake (cache-backend-redis:make-fake-redis))
       (c (cache-backend-redis:make-redis-cache-backend
           :send-fn (cache-backend-redis:fake-redis-send-fn fake))))
  (stack-cache:cache-put c "k" "v")
  (stack-cache:cache-get c "k"))
```

CAS uses `EVAL` of `+cas-lua+` (`GET` compare then `SET`). Alternative: `SET key value NX` plus a version key.

Encoded types: simple string, error, integer, blob, array, null, map, set, bool, double.

## License

MIT — see [LICENSE](LICENSE).
