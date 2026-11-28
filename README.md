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
| `:url` | | `redis://[[user]:password@]host[:port][/db][?db=N]`; `rediss://` turns TLS on. Explicit keywords win over URL parts (see [URLs](#urls)) |
| `:username` `:password` | `nil` | sent as `HELLO 3 AUTH user pass` (`default` user when only a password is given; `AUTH user ""` for a username alone, an ACL `nopass` user, as in redis-py). The password (a string, octets, an integer or a symbol) never shows in a backtrace: it travels as `#<SECRET ***>` |
| `:db` | `0` | `SELECT db` after the handshake |
| `:tls` `:tls-verify` | `nil` `:required` | cl+ssl client stream with SNI and hostname check; the OS trust store comes from cl-stack-ssl |
| `:client-name` | `nil` | `HELLO ... SETNAME` |
| `:connect-timeout` | `5` | seconds for the connect, the TLS handshake and each command: a server that does not answer in time is a `cache-connection-error`, not a hang (on SBCL a deadline per command, writes included: outside Windows the plain-TCP socket is non-blocking once connected, so a server that stops reading cannot block a large write; elsewhere usocket's socket timeout per wait). Not over plain TCP on Windows, where SBCL reads a socket with no timeout ([#9](https://github.com/egao1980/cache-backend-redis/issues/9)) |
| `:retries` | `1` | resends after a dropped socket or a refused reconnect |
| `:retry-delay` | `1/10` | seconds before the second resend, doubled for each later one (capped at 2 s); the first resend goes at once |
| `:stream` | | an `(unsigned-byte 8)` stream the caller owns (never closed by the backend) |
| `:send-fn` | | a function of the command list returning the decoded reply (tests) |

A caller's channel runs no handshake, so `:stream` and `:send-fn` are refused (`cache-error`) with each other and with any connection option: `:host`, `:url`, and a non-`nil` `:port`, `:username`, `:password`, `:db`, `:tls`, `:tls-verify` or `:client-name`.

On a server without `HELLO` (Redis 5) the backend falls back to `AUTH`. A `:username` with no `:password` sends `AUTH user ""` there too, so Redis 5 refuses it at connect (as redis-py does) instead of a `NOAUTH` on the first command.

### URLs

`parse-redis-url` reads a URL the way redis-py's `from_url` does:

- an IPv6 address goes in brackets, `redis://[::1]:6379/0`, and is connected to without them;
- `?db=N` wins over the path (`redis://h/2?db=3` is db 3); a blank `?db=` is ignored and the first one counts. Any other query parameter is refused, not ignored. A fragment is ignored;
- user, password, host, path and query are percent-decoded, so `/ ? # @ %` in a password are written `%2F %3F %23 %40 %25`.

A bad URL signals `cache-error`. Its report masks what may be a secret: the password (all of the userinfo when it has no colon), every query value but `db`'s, and the fragment. A port that is not a number, or a port after an empty host (`redis://:password` with no `@host`), is taken for a password: it and the rest of the URL are masked. A URL with no `://` (`redis:/:password`) is masked past its scheme (`redis:***`).

### Connection

The connection opens on first use. A server without `HELLO` (Redis 5) gets `AUTH` / `CLIENT SETNAME` and stays on RESP2, which the decoder reads too; it has no ACL users, so when it refuses `AUTH user pass` for its arity the backend sends `AUTH pass`, as redis-py does (`redis://default:pass@host` URLs work there). A username without a password sends no `AUTH` to it. RESP3 push frames (client tracking, pub/sub) are out-of-band: they are skipped, never taken as a command's reply. `close` releases the socket and returns `T` if one was open; the backend reconnects if it is used again. `with-redis-cache` closes on exit. Commands are serialized by a lock, so one backend can be shared between threads.

## Errors

| situation | condition |
|---|---|
| refused / timed out connect, dropped socket, TLS failure (a certificate that does not name the host included), EOF | `cache-connection-error`, the usocket / stream / cl+ssl condition in `cause` |
| `-LOADING` reply (a restarting server still loading its dataset) | `cache-connection-error`: the connection is dropped and the command resent, as for a dropped socket |
| server error reply (`WRONGPASS`, `SET ... EX 0`, `INCR` of `"abc"`, script error) | `cache-error` with the server's text and the key |
| CAS mismatch | `cache-cas-mismatch` with restart `use-value` |

Once the backend has had a connection, a dropped socket is retried transparently on a new one, up to `:retries` resends. Each resend counts, so `:retries 3` survives a server that drops the idle connection and then the first reconnect while it restarts; a refused reconnect is retried too, after the `:retry-delay` backoff, in that call and in every later one: a call that gave up while the server restarts does not make the next call fail fast. A backend that never connected fails after its first connect attempt (wrong host or port). How long a refused connect takes depends on the platform: macOS reports it at once; on Linux usocket waits out `:connect-timeout` ([cl-stack-systems#81](https://github.com/egao1980/cl-stack-systems/issues/81)); Windows takes about 2 s, and usocket's SBCL backend applies no connect timeout there.

Resends are at-least-once, as in redis-py: a command whose reply was lost may run twice, so an `INCR` can count twice and a CAS that did swap can report `cache-cas-mismatch`. A command whose reply is not read to the end (an error half-way, a timeout or another non-local exit around the call) drops the connection, so the rest of that reply never reaches the next command. A caller's `:stream` cannot be dropped: after such an error, discard it. Past that the `cache-connection-error` carries a `reconnect` restart:

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

The codec reads and writes an `(unsigned-byte 8)` (or bivalent) stream. Text is UTF-8 through `encoding-protocol`: a bulk string is written as its UTF-8 octet count followed by the octets, and a reply is read by its octet length and then decoded, so non-ASCII keys and values round-trip and a value containing CRLF cannot start a second command. Numbers on the wire (lengths, `EX` / `PX`, `INCRBY`, `SELECT`, values) are always decimal, whatever the caller's `*print-base*` / `*print-radix*`; a float value goes without an exponent marker (`0.1d0` → `0.1`) and a symbol upper-case, whatever `*read-default-float-format*` / `*print-case*`. A ratio or complex value (`1/3`) has no text Redis reads as that number, so it signals `cache-error` before anything is sent: pass `(float x)` or a string of your own. Simple strings and errors containing CR or LF are refused. Invalid UTF-8 in a reply signals `cache-error` with a `use-value` restart. So do a reply nested deeper than `*max-nesting-depth*` (64) levels and a double that is not a decimal number, `inf` or `-inf`; doubles are parsed without interning or reader macros.

- `encode-resp3-command parts` → octets; `encode-resp3 value stream`, `decode-resp3 stream &key skip-push` on binary streams (`:skip-push t` reads past push frames to the next value).
- `encode-resp3-to-octets` / `decode-resp3-from-octets`; `encode-resp3-to-string` / `decode-resp3-from-string` treat the string as the UTF-8 text of the wire.

## Tests

`cl-repo test`. Live tests (`tests/connect-test.lisp`, `tests/live-test.lisp`) skip without a password-protected Valkey on `127.0.0.1:16390`:

```sh
docker run -d --name valkey-test -p 127.0.0.1:16390:6379 valkey/valkey:8 \
  valkey-server --save "" --appendonly no --requirepass s3cret
```

The TLS test also wants `CACHE_REDIS_TEST_TLS_PORT` and `CACHE_REDIS_TEST_CA` (see the file header). `tests/b2-adv1-test.lisp` also reads `CACHE_REDIS_TEST_REDIS5_PORT` / `CACHE_REDIS_TEST_REDIS7_PORT` (`redis:5` / `redis:7` with the same `--requirepass`) and `CACHE_REDIS_TEST_TLS_WRONG_NAME_PORT` (a TLS Valkey whose certificate, signed by the test CA, names only `other.example`).

## License

MIT — see [LICENSE](LICENSE).
