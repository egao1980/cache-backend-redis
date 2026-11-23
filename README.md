# cache-backend-redis

RESP3 Redis / Valkey backend for [`cache-protocol`](https://github.com/egao1980/cache-protocol). Give it a host (or a `redis://` URL) and it owns the connection: TCP through usocket, TLS through [`cl-stack-ssl`](https://github.com/egao1980/cl-stack-ssl) (cl+ssl), `HELLO 3` with `AUTH`, `SELECT`, reconnect after a dropped socket, `close`. Or hand it a stream / `send-fn` you manage. Unit tests need no live Valkey: they use a fake RESP server, in memory or on an ephemeral TCP port.

```lisp
(asdf:load-system "cache-backend-redis")

;; redis-py: redis.Redis(host="127.0.0.1", port=6379, password="s3cret", db=3)
(cache-backend-redis:with-redis-cache (c :host "127.0.0.1" :port 6379
                                         :password "s3cret" :db 3)
  (stack-cache:cache-put c "k" "v" :ttl 60)
  (stack-cache:cache-get c "k")
  (stack-cache:cache-incr c "n")
  (stack-cache:cache-cas c "k" "v" "v2" :ttl 30))

;; redis.from_url("rediss://app:secret@cache.example.com:6380/0")
(defvar *cache* (cache-backend-redis:make-redis-cache-backend
                 :url "rediss://app:secret@cache.example.com:6380/0"))
(stack-cache:cache-get *cache* "k")
(close *cache*)
```

## Connecting

`make-redis-cache-backend` keywords:

| keyword | default | |
|---|---|---|
| `:host` `:port` | — `6379` | the backend owns a connection |
| `:url` | | `redis://[[user]:password@]host[:port][/db]`; `rediss://` turns TLS on. Explicit keywords win over URL parts |
| `:username` `:password` | `nil` | sent as `HELLO 3 AUTH user pass` (`default` user when only a password is given) |
| `:db` | `0` | `SELECT db` after the handshake |
| `:tls` `:tls-verify` | `nil` `:required` | cl+ssl client stream with SNI and hostname check; the OS trust store comes from cl-stack-ssl |
| `:client-name` | `nil` | `HELLO ... SETNAME` |
| `:connect-timeout` | `5` | seconds |
| `:retries` | `1` | resends after a dropped socket |
| `:stream` | | an `(unsigned-byte 8)` stream the caller owns (never closed by the backend) |
| `:send-fn` | | a function of the command list returning the decoded reply (tests) |

The connection opens on first use. A server without `HELLO` (Redis 5) gets `AUTH` / `CLIENT SETNAME` and stays on RESP2, which the decoder reads too. `close` releases the socket and returns `T` if one was open; the backend reconnects if it is used again. `with-redis-cache` closes on exit. Commands are serialized by a lock, so one backend can be shared between threads.

## Errors

| situation | condition |
|---|---|
| refused / timed out connect, dropped socket, TLS failure, EOF | `cache-connection-error`, the usocket / stream / cl+ssl condition in `cause` |
| server error reply (`WRONGPASS`, `SET ... EX 0`, `INCR` of `"abc"`, script error) | `cache-error` with the server's text and the key |
| CAS mismatch | `cache-cas-mismatch` with restart `use-value` |

A dropped socket is retried transparently when the connection had been up (up to `:retries` times; like redis-py, a command whose reply was lost may run twice). Past that the `cache-connection-error` carries a `reconnect` restart:

```lisp
(handler-bind ((stack-cache:cache-connection-error
                 (lambda (e) (invoke-restart (find-restart 'cache-backend-redis:reconnect e)))))
  (stack-cache:cache-get c "k"))
```

## Commands

- `cache-put` → `SET key value [EX n | PX ms]`: an integer `:ttl` is `EX`, a fraction is `PX` (`:ttl 1.5` → `PX 1500`).
- `cache-incr` → `INCR` / `INCRBY` (Redis keeps the key's TTL).
- `cache-cas` → `EVAL` of `+cas-lua+` with `ARGV` = absent-flag, expected, new, PX ms or `""`. Expected `nil` matches a missing key (create-if-absent), and `:ttl` sets the new value's expiry, as on `in-memory-ttl-cache`.

Encoded types: simple string, error, integer, blob, array, null, map, set, bool, double. Decoded also: verbatim string, blob error, big number, push, attribute (skipped).

## Wire encoding

The codec reads and writes an `(unsigned-byte 8)` (or bivalent) stream. Text is UTF-8 through `encoding-protocol`: a bulk string is written as its UTF-8 octet count followed by the octets, and a reply is read by its octet length and then decoded, so non-ASCII keys and values round-trip and a value containing CRLF cannot start a second command. Simple strings and errors containing CR or LF are refused. Invalid UTF-8 in a reply signals `cache-error` with a `use-value` restart.

- `encode-resp3-command parts` → octets; `encode-resp3 value stream`, `decode-resp3 stream` on binary streams.
- `encode-resp3-to-octets` / `decode-resp3-from-octets`; `encode-resp3-to-string` / `decode-resp3-from-string` treat the string as the UTF-8 text of the wire.

## Tests

`cl-repo test`. Live tests (`tests/connect-test.lisp`, `tests/live-test.lisp`) skip without a password-protected Valkey on `127.0.0.1:16390`:

```sh
docker run -d --name valkey-test -p 127.0.0.1:16390:6379 valkey/valkey:8 \
  valkey-server --save "" --appendonly no --requirepass s3cret
```

The TLS test also wants `CACHE_REDIS_TEST_TLS_PORT` and `CACHE_REDIS_TEST_CA` (see the file header).

## License

MIT — see [LICENSE](LICENSE).
