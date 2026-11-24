(in-package #:cache-backend-redis/tests)

;;; Round 3 (#3, #7): a backend that has had a connection keeps retrying
;;; refused reconnects in later calls too; percent-encoded passwords parse,
;;; unescaped / ? # @ % in a password are refused with the password masked.

(deftest-parametrize percent-encoded-passwords-parse
    ((url password)
     ("redis://:s3cret%2Fx%2By%3D%3D@cache.internal:6379/0" "s3cret/x+y==")
     ("redis://app:s3cret%231@cache.internal/0" "s3cret#1")
     ("redis://app:s3cret%3F1@cache.internal/0" "s3cret?1")
     ("redis://app:p%40ss%25@cache.internal/0" "p@ss%")
     ("redis://app:s3cret+/=@cache.internal/2" nil))
  (let ((got (handler-case (parse-redis-url url)
               (cache-protocol:cache-error () :refused))))
    (if password
        (progn
          (ok (equal password (getf got :password))
              (format nil "~A has password ~S" url password))
          (ok (equal "cache.internal" (getf got :host)) "the host is the one after the @"))
        (ok (eq :refused got)
            (format nil "an unescaped / in the password is refused, not misrouted (got ~S)"
                    got)))))

(deftest unescaped-password-errors-say-how-to-escape
  (let ((e (handler-case (progn (parse-redis-url "redis://app:a/b@cache.internal/0") nil)
             (cache-protocol:cache-error (e) e))))
    (ok e "refused")
    (ok (and e (search "%2F" (princ-to-string e)))
        (format nil "the report says how to escape it: ~A" e))
    (ok (and e (not (search "a/b" (princ-to-string e)))) "and masks the password")))

(deftest a-backend-that-never-connected-still-fails-at-once
  ;; :retries applies once the backend has had a connection; a wrong port
  ;; is reported after one connect attempt however many retries are allowed.
  ;; How long one refused connect takes depends on the platform: macOS
  ;; reports it at once, usocket on Linux waits out the connect timeout
  ;; (egao1980/cl-stack-systems#81), Windows takes about 2 s (it retries the
  ;; SYN, and usocket's SBCL backend applies no connect timeout there). So
  ;; the timeout is explicit and small, and the bound is one measured
  ;; refused connect plus a margin, well under the 1 s retry delay.
  (flet ((seconds-since (start)
           (/ (- (get-internal-real-time) start) internal-time-units-per-second)))
    (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t))
           (port (usocket:get-local-port listener))
           (timeout 1/2))
      (usocket:socket-close listener)
      (let ((one-connect (let ((start (get-internal-real-time)))
                           (handler-case
                               (usocket:socket-close
                                (usocket:socket-connect "127.0.0.1" port
                                                        :element-type '(unsigned-byte 8)
                                                        :timeout timeout))
                             ((or usocket:socket-error usocket:ns-error) () nil))
                           (seconds-since start))))
        (with-redis-cache (c :host "127.0.0.1" :port port :connect-timeout timeout
                             :retries 5 :retry-delay 1)
          (let ((start (get-internal-real-time)))
            (ok (signals (cache-protocol:cache-get c "k")
                  'cache-protocol:cache-connection-error))
            (let ((elapsed (seconds-since start)))
              (ok (< elapsed (+ one-connect 1/2))
                  (format nil "no backoff for a backend that never connected ~
                               (~,2F s; one refused connect takes ~,2F s here)"
                          elapsed one-connect)))))))))
