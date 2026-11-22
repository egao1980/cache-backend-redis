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

## Wire encoding

The codec reads and writes an `(unsigned-byte 8)` (or bivalent) stream. Text is UTF-8 through `encoding-protocol`: a bulk string is written as its UTF-8 octet count followed by the octets, and a reply is read by its octet length and then decoded, so non-ASCII keys and values round-trip and a value containing CRLF cannot start a second command. Simple strings and errors containing CR or LF are refused. Invalid UTF-8 in a reply signals `cache-error` with a `use-value` restart.

- `encode-resp3-command parts` → octets; `encode-resp3 value stream`, `decode-resp3 stream` on binary streams.
- `encode-resp3-to-octets` / `decode-resp3-from-octets`; `encode-resp3-to-string` / `decode-resp3-from-string` treat the string as the UTF-8 text of the wire.

## License

MIT — see [LICENSE](LICENSE).
