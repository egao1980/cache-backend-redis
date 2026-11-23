(in-package #:cache-backend-redis/tests)

;;; Live checks of #4 (error replies), #5 (CAS :ttl / expected NIL) and TLS
;;; against Valkey. Same server as connect-test.lisp; skip without it.
;;; TLS needs a second Valkey with TLS on CACHE_REDIS_TEST_TLS_PORT and the
;;; CA that signed its certificate (for 127.0.0.1) in CACHE_REDIS_TEST_CA:
;;;   docker run -d -p 127.0.0.1:16391:6379 -v $PWD/certs:/certs:ro valkey/valkey:8 \
;;;     valkey-server --save "" --requirepass s3cret --port 0 --tls-port 6379 \
;;;     --tls-cert-file /certs/cert.pem --tls-key-file /certs/key.pem \
;;;     --tls-ca-cert-file /certs/ca.pem --tls-auth-clients no

(deftest live-error-replies-signal
  (with-side (side)
    (let ((key (%key "errors")))
      (with-redis-cache (c :host (%live-host) :port (%live-port)
                           :password (%live-password))
        (ok (signals (cache-protocol:cache-put c key "v" :ttl 0) 'cache-protocol:cache-error)
            "SET EX 0 is refused and signals")
        (cache-protocol:cache-put c key "abc")
        (ok (signals (cache-protocol:cache-incr c key) 'cache-protocol:cache-error)
            "INCR of \"abc\" signals")
        (ok (equal "abc" (cache-protocol:cache-get c key)) "the connection is still usable")))))

(deftest live-fractional-ttl
  (with-side (side)
    (let ((key (%key "px")))
      (with-redis-cache (c :host (%live-host) :port (%live-port)
                           :password (%live-password))
        (cache-protocol:cache-put c key "v" :ttl 0.4)
        (let ((pttl (%side side "PTTL" key)))
          (ok (and (integerp pttl) (< 0 pttl 401)) (format nil "PTTL ~S ≤ 400 ms" pttl)))
        (sleep 0.5)
        (ok (null (cache-protocol:cache-get c key)) "gone after 0.5 s")))))

(deftest live-cas-expected-nil-and-ttl
  (with-side (side)
    (let ((key (%key "cas")))
      (with-redis-cache (c :host (%live-host) :port (%live-port)
                           :password (%live-password))
        (ok (equal "first" (cache-protocol:cache-cas c key nil "first"))
            "expected NIL matches the missing key on a real server")
        (ok (signals (cache-protocol:cache-cas c key nil "second")
                     'cache-protocol:cache-cas-mismatch)
            "and not a present one")
        (ok (equal "next" (cache-protocol:cache-cas c key "first" "next" :ttl 100)))
        (let ((ttl (%side side "TTL" key)))
          (ok (and (integerp ttl) (< 90 ttl 101)) (format nil "CAS :ttl 100 → TTL ~S" ttl)))
        (cache-protocol:cache-cas c key "next" "last")
        (ok (eql -1 (%side side "TTL" key)) "CAS without :ttl clears the expiry, like SET")))))

(defun %tls-port ()
  (let ((v (uiop:getenv "CACHE_REDIS_TEST_TLS_PORT")))
    (and v (plusp (length v)) (parse-integer v))))

(defvar *test-ca-loaded* nil)

(defun %trust-test-ca ()
  (let ((ca (uiop:getenv "CACHE_REDIS_TEST_CA")))
    (when (and ca (plusp (length ca)) (probe-file ca))
      (unless *test-ca-loaded*
        (cl+ssl:ssl-load-global-verify-locations ca)
        (setf *test-ca-loaded* t))
      t)))

(deftest live-tls-url
  (cond
    ((not (and (%tls-port) (%trust-test-ca)))
     (skip "CACHE_REDIS_TEST_TLS_PORT / CACHE_REDIS_TEST_CA not set (see tests/live-test.lisp)"))
    (t
     (let ((key (%key "tls")))
       (with-redis-cache (c :url (format nil "rediss://:~A@127.0.0.1:~D/2"
                                         (%live-password) (%tls-port)))
         (cache-protocol:cache-put c key "over-tls")
         (ok (equal "over-tls" (cache-protocol:cache-get c key)) "rediss:// round trip")
         (ok (redis-cache-backend-tls c)))
       (ok (signals (let ((c (make-redis-cache-backend :host "127.0.0.1" :port (%tls-port)
                                                       :password (%live-password))))
                      (unwind-protect (cache-protocol:cache-get c key)
                        (close c)))
                    'cache-protocol:cache-error)
           "plain TCP to the TLS port fails with a cache error")))))
