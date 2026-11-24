(in-package #:cache-backend-redis/tests)

;;; Round-2 evidence against PR #6.
;;;
;;; 1. README: "Once the backend has had a connection, a dropped socket is
;;;    retried transparently on a new one, up to :retries resends ... a
;;;    refused reconnect is retried too, after the :retry-delay backoff. A
;;;    backend that never connected fails at once (wrong host or port)."
;;;    MAKE-REDIS-CACHE-BACKEND's docstring says the same. %CALL computes
;;;    ESTABLISHED per call from CONNECTED-P at entry, and a call that gave
;;;    up has dropped the socket, so the NEXT call to a server that is still
;;;    restarting fails at once, as if the backend had never connected.
;;;    redis-py's Retry applies to every command.
;;;
;;; 2. #7 (fixed on this PR): "a URL quri cannot parse becomes a masked
;;;    cache-error". %REDACT-URL ends the authority at the first / ? or #,
;;;    so a password holding one of them (base64 passwords hold / + =) is
;;;    printed in full in "malformed URL: ...", and a stray % in a password
;;;    escapes as QURI.ERROR:URL-DECODING-ERROR, not a CACHE-ERROR.

(deftest a-refused-connect-in-the-next-call-is-retried-too
  ;; The server is down longer than one call's backoff (0 + 0.2 + 0.4 s):
  ;; the first GET gives up. The server is back 0.3 s into the second GET,
  ;; inside that call's own backoff window.
  (let* ((dispatch (%fake-dispatch))
         (srv (start-flaky-server dispatch '(2)))
         (port (ss-port srv))
         (again nil))
    (unwind-protect
        (with-redis-cache (c :host "127.0.0.1" :port port
                             :retries 3 :retry-delay 1/5)
          (%drop-first-connection c srv)
          (stop-scripted-server srv)
          (ok (signals (cache-protocol:cache-get c "k")
                'cache-protocol:cache-connection-error)
              "the first GET gives up: the server is still down")
          (let ((restarter (bt2:make-thread
                            (lambda ()
                              (sleep 0.3)
                              (setf again (start-flaky-server dispatch nil :port port)))
                            :name "redis-restart"))
                (start (get-internal-real-time)))
            (let ((got (handler-case (cache-protocol:cache-get c "k")
                         (cache-protocol:cache-connection-error (e) e)))
                  (elapsed (/ (- (get-internal-real-time) start)
                              internal-time-units-per-second)))
              (bt2:join-thread restarter)
              (ok (equal "v" got)
                  (format nil "the second GET waits out the restart too (got ~A after ~,2F s)"
                          got elapsed)))))
      (when again (stop-scripted-server again)))))

(deftest-parametrize bad-url-errors-mask-special-character-passwords
    ((url)
     ("redis://:s3cret/x+y==@cache.internal:6379/0")
     ("redis://app:s3cret#1@cache.internal/0")
     ("redis://app:s3cret?1@cache.internal/0")
     ("redis://:s3cret%zz@cache.internal/0"))
  (let ((e (handler-case (progn (parse-redis-url url) nil)
             (error (e) e))))
    (ok (typep e 'cache-protocol:cache-error)
        (format nil "a bad URL signals cache-error (got ~A)" (type-of e)))
    (ok (and e (not (search "s3cret" (princ-to-string e))))
        (format nil "no password in the report: ~A" e))))
