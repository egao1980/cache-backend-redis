(in-package #:cache-backend-redis/tests)

;;; Evidence, round 3 (#3 connect by URL): IPv6 literals and ?db=.
;;;
;;; redis-py 6.4.0 (checked 2026-11-24 against Valkey 8):
;;;   redis.from_url("redis://[::1]:6379/0")        host '::1', connects
;;;   redis.from_url("redis://:pw@[::1]:6379/2")    host '::1', connects
;;;   redis.from_url("redis://:pw@127.0.0.1:6379?db=3")   SELECTs db 3
;;; Here the host keeps its brackets ("[::1]", which usocket cannot
;;; resolve), a password next to an IPv6 literal is refused as
;;; "malformed URL" with an escaping hint that does not apply, and ?db=3
;;; is dropped without a word, so the backend writes to db 0.

(deftest-parametrize ipv6-url-host-has-no-brackets
    ((url host port)
     ("redis://[::1]:6379/0" "::1" 6379)
     ("redis://[::1]/0" "::1" 6379)
     ("rediss://[2001:db8::1]:6380/0" "2001:db8::1" 6380))
  (let ((got (handler-case (parse-redis-url url)
               (cache-protocol:cache-error (e) (list :refused (princ-to-string e))))))
    (ok (and (equal host (getf got :host)) (eql port (getf got :port)))
        (format nil "~A is host ~S port ~D, as in redis-py (got ~S)" url host port got))))

(deftest ipv6-url-with-a-password-parses
  (let ((got (handler-case (parse-redis-url "redis://app:pw@[::1]:6379/2")
               (cache-protocol:cache-error (e) (list :refused (princ-to-string e))))))
    (ok (and (equal "::1" (getf got :host))
             (equal "app" (getf got :username))
             (equal "pw" (getf got :password))
             (eql 2 (getf got :db)))
        (format nil "redis://app:pw@[::1]:6379/2 parses as in redis-py (got ~S)" got))))

(deftest db-query-is-read-or-refused
  ;; redis-py reads ?db=3. Dropping it silently sends every write to db 0.
  (let ((got (handler-case (parse-redis-url "redis://cache.internal:6379?db=3")
               (cache-protocol:cache-error () :refused))))
    (ok (or (eq :refused got) (eql 3 (getf got :db)))
        (format nil "?db=3 selects db 3 or is refused, not db 0 (got ~S)" got))))

(deftest connect-by-ipv6-url
  ;; Control first: the same server by :host "::1". Without a Valkey on
  ;; ::1 the test skips.
  (with-side (side)
    (%side side "DEL" "r3-v6")
    (let ((control (handler-case
                       (with-redis-cache (c :host "::1" :port (%live-port)
                                            :password (%live-password))
                         (cache-protocol:cache-put c "r3-v6" "ok")
                         (cache-protocol:cache-get c "r3-v6"))
                     (error () nil))))
      (if (not (equal "ok" control))
          (skip "the live Valkey does not answer on ::1")
          (let ((got (handler-case
                         (with-redis-cache (c :url (format nil "redis://[::1]:~D/0" (%live-port))
                                              :password (%live-password))
                           (cache-protocol:cache-get c "r3-v6"))
                       (error (e) (list :error (princ-to-string e))))))
            (ok (equal "ok" got)
                (format nil ":host \"::1\" works; redis://[::1]:~D/0 got ~S"
                        (%live-port) got)))))))
