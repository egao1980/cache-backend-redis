(in-package #:cache-backend-redis/tests)

;;; Round 4 (#7, #8, and three breaks found while fixing them): redis://
;;; URLs as redis-py reads them (IPv6 literals, ?db=, other query
;;; parameters); errors that mask every part that may hold a secret; a reply
;;; abandoned half-way that must not reach the next command; and a server
;;; that never answers, over TCP and TLS.

;;; --- URLs (#8) -------------------------------------------------------------
;;;
;;; redis-py 8.2.0 (redis.connection.parse_url / from_url), 2026-11-24:
;;;   redis://h:6379/2?db=3        db 3   (?db= wins over the path)
;;;   redis://h/2?db=              db 2   (a blank ?db= is ignored)
;;;   redis://h?db=1&db=2          db 1   (the first one counts)
;;;   redis://h?db=%33, ?db=+3     db 3
;;;   redis://h?db=abc             ValueError
;;;   redis://h?foo=bar            TypeError in from_url (unknown keyword)
;;;   redis://[fe80::1%25eth0]/0   host fe80::1%eth0
;;;   redis://my%2Dhost/0          host my-host

(deftest-parametrize url-db-as-in-redis-py
    ((url db)
     ("redis://cache.internal:6379?db=3" 3)
     ("redis://h:6379/2?db=3" 3)
     ("redis://h/2?db=" 2)
     ("redis://h/?db=4" 4)
     ("redis://h?db=1&db=2" 1)
     ("redis://h?db=%33" 3)
     ("redis://h?db=+3" 3)
     ("redis://h:6379?db=3#f" 3)
     ("redis://h?db=abc" :refused)
     ("redis://h?db=-1" :refused)
     ("redis://h?foo=bar" :refused)
     ("redis://h/0?db=1;db=2" :refused))
  (let ((got (handler-case (getf (parse-redis-url url) :db)
               (cache-protocol:cache-error () :refused))))
    (ok (eql db got) (format nil "~A → db ~S (got ~S)" url db got))))

(deftest-parametrize url-hosts-and-ports
    ((url host)
     ("redis://[fe80::1%25eth0]:6379/0" "fe80::1%eth0")
     ("redis://[::FFFF:1.2.3.4]/0" "::FFFF:1.2.3.4")
     ("redis://my%2Dhost/0" "my-host")
     ("redis://10.0.0.5/0" "10.0.0.5")
     ("redis://[zzz]/0" :refused)
     ("redis://[::1/0" :refused)
     ("redis://[::1]x/0" :refused)
     ("redis://h:6379:1/0" :refused)
     ("redis://h%20x/0" :refused)
     ("redis://h:0/0" :refused)
     ("redis://h:65536/0" :refused))
  (let ((got (handler-case (getf (parse-redis-url url) :host)
               (cache-protocol:cache-error () :refused))))
    (ok (equal host got) (format nil "~A → host ~S (got ~S)" url host got))))

(deftest-parametrize url-errors-mask-every-secret
    ((url)
     ("redis://h:6379/0?password=s3cret")
     ("redis://h/0?db=x&password=s3cret")
     ;; @host forgotten: the password lands where the port goes.
     ("redis://:s3cret")
     ("redis://h:s3cret/0")
     ("redis://h/abc#s3cret")
     ;; No colon in the userinfo: all of it is masked.
     ("redis://s3cret@h/abc")
     ("redis://h?s3cret"))
  (let ((text (handler-case (progn (parse-redis-url url) "no error")
                (cache-protocol:cache-error (e) (princ-to-string e)))))
    (ok (string/= "no error" text) (format nil "~A is refused" url))
    (ok (not (search "s3cret" text)) (format nil "no secret in: ~A" text))))

(deftest escaping-hint-only-when-the-user-or-password-needs-it
  (flet ((report (url)
           (handler-case (progn (parse-redis-url url) "no error")
             (cache-protocol:cache-error (e) (princ-to-string e)))))
    (ok (search "%2F" (report "redis://app:a/b@h/0")) "an unescaped / in the password: hint")
    (ok (not (search "%2F" (report "redis://app:pw@h/abc")))
        "a bad db next to a plain password: no hint")
    (ok (not (search "%2F" (report "redis://app:pw@h:6379:1/0")))
        "a bad host next to a plain password: no hint")))

(deftest ipv6-endpoints-are-bracketed
  (let ((c (make-redis-cache-backend :url "redis://[::1]:1/3" :connect-timeout 1/2)))
    (ok (search "redis://[::1]:1/3" (princ-to-string c)) (format nil "printed: ~A" c))
    (let ((e (handler-case (progn (cache-protocol:cache-get c "k") nil)
               (cache-protocol:cache-connection-error (e) e))))
      (ok (and e (search "[::1]:1" (princ-to-string e))) (format nil "report: ~A" e)))))

;;; --- an abandoned reply ------------------------------------------------------
;;;
;;; A reply the client stops reading half-way stays on the socket. If the
;;; connection were kept, the next command would read it as its own reply
;;; (redis-py CVE-2023-28858 has this shape). The backend drops it instead.

(defun %nested (depth leaf)
  (if (zerop depth) leaf (list (%nested (1- depth) leaf))))

(deftest a-reply-refused-half-way-never-reaches-the-next-command
  (let ((fake (make-fake-redis)))
    (fake-redis-dispatch fake '("SET" "other" "OTHER"))
    (with-scripted-server (srv (lambda (parts)
                                 (if (equal parts '("GET" "deep"))
                                     (%nested (+ 10 *max-nesting-depth*) "DEEP")
                                     (fake-redis-dispatch fake parts))))
      (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv))
        (ok (signals (cache-protocol:cache-get c "deep") 'cache-protocol:cache-error)
            "a reply nested too deep signals cache-error")
        (ok (not (redis-cache-backend-connected-p c)) "and drops the connection")
        (ok (equal "OTHER" (cache-protocol:cache-get c "other"))
            "the next GET gets its own reply")))))

(deftest a-call-abandoned-by-a-timeout-does-not-leak-its-reply
  (let ((fake (make-fake-redis)))
    (fake-redis-dispatch fake '("SET" "slow" "SLOW"))
    (fake-redis-dispatch fake '("SET" "other" "OTHER"))
    (with-scripted-server (srv (lambda (parts)
                                 (when (equal parts '("GET" "slow"))
                                   (sleep 0.4))
                                 (fake-redis-dispatch fake parts)))
      (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv))
        (cache-protocol:cache-get c "other")
        (ok (eq :timed-out (handler-case (bt2:with-timeout (0.1)
                                           (cache-protocol:cache-get c "slow"))
                             (bt2:timeout () :timed-out)))
            "the caller gives up on the slow GET")
        (sleep 0.5)
        (ok (equal "OTHER" (cache-protocol:cache-get c "other"))
            "the next GET gets its own reply, not the slow one")))))

;;; --- a server that never answers ---------------------------------------------

(defun %call-with-limit (seconds fn)
  "FN's value, or the condition it signalled, from another thread; :HUNG
when it takes longer than SECONDS (the thread is then destroyed)."
  (let* ((result '%pending)
         (thread (bt2:make-thread
                  (lambda ()
                    (setf result (handler-case (funcall fn)
                                   (serious-condition (e) e))))
                  :name "limited-call")))
    (loop repeat (* seconds 20)
          while (eq result '%pending)
          do (sleep 0.05))
    (cond ((eq result '%pending)
           (bt2:destroy-thread thread)
           :hung)
          (t (bt2:join-thread thread)
             result))))

(defun %call-with-silent-server (fn)
  "Call FN with the port of a listener on 127.0.0.1 that accepts
connections and never writes."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                        :element-type '(unsigned-byte 8)))
         (accepted '())
         (stopping nil)
         (acceptor (bt2:make-thread
                    (lambda ()
                      (loop until stopping
                            when (usocket:wait-for-input listener :timeout 0.05 :ready-only t)
                              do (push (usocket:socket-accept listener) accepted)))
                    :name "silent-redis")))
    (unwind-protect (funcall fn (usocket:get-local-port listener))
      (setf stopping t)
      (bt2:join-thread acceptor)
      (mapc #'usocket:socket-close accepted)
      (usocket:socket-close listener))))

(deftest-parametrize a-silent-server-is-a-connection-error-not-a-hang
    ((tls)
     (nil)
     (t))
  ;; Over TCP the socket stream's timeout ends the wait. Over TLS cl+ssl
  ;; reads the socket itself, so only the I/O deadline does.
  (%call-with-silent-server
   (lambda (port)
     (let ((got (%call-with-limit 5 (lambda ()
                                      (with-redis-cache (c :host "127.0.0.1" :port port
                                                           :tls tls :tls-verify nil
                                                           :connect-timeout 1/2)
                                        (cache-protocol:cache-get c "k"))))))
       (ok (typep got 'cache-protocol:cache-connection-error)
           (format nil "~:[TCP~;TLS~]: a server that never answers → connection error (got ~A)"
                   tls got))))))

;;; --- CAS and RESP3 details -----------------------------------------------------

(deftest cas-error-names-the-key
  (let* ((fake (make-fake-redis))
         (c (make-redis-cache-backend :send-fn (fake-redis-send-fn fake))))
    (cache-protocol:cache-put c "k" "old")
    (let ((e (handler-case (progn (cache-protocol:cache-cas c "k" "old" "new" :ttl 0) nil)
               (cache-protocol:cache-error (e) e))))
      (ok e "a refused CAS :ttl signals cache-error")
      (ok (and e (equal "k" (cache-protocol:cache-error-key e)))
          (format nil "with the key, not the script (got ~S)"
                  (and e (cache-protocol:cache-error-key e)))))))

(deftest-parametrize resp3-doubles
    ((wire expected)
     (",1.5" 1.5d0)
     (",-2" -2d0)
     (",1e3" 1000d0)
     (",.5" 0.5d0)
     (",inf" :inf)
     (",1e999" :refused)
     (",nan" :refused)
     (",zz-not-a-number" :refused)
     (",#.(error \"read-eval\")" :refused))
  (let ((got (handler-case (decode-resp3-from-string
                            (format nil "~A~C~C" wire #\Return #\Linefeed))
               (cache-protocol:cache-error () :refused))))
    (ok (if (eq expected :inf)
            (and (floatp got) (> got most-positive-double-float))
            (eql expected got))
        (format nil "~A → ~S (got ~S)" wire expected got))))

(deftest resp3-double-interns-nothing
  (let ((name "ZZ-RESP3-DOUBLE-PROBE"))
    (handler-case (decode-resp3-from-string (format nil ",~A~C~C" name #\Return #\Linefeed))
      (cache-protocol:cache-error () nil))
    (ok (null (find-symbol name *package*)) "no symbol interned in *PACKAGE*")
    (ok (null (find-symbol name "CL-USER")) "nor in CL-USER")))

(deftest resp3-nesting-is-bounded
  (let ((limit *max-nesting-depth*))
    (ok (equal (%nested (1- limit) "x")
               (decode-resp3-from-octets (encode-resp3-to-octets (%nested (1- limit) "x"))))
        "a value at the deepest allowed level decodes")
    (ok (signals (decode-resp3-from-octets (encode-resp3-to-octets (%nested limit "x")))
                 'cache-protocol:cache-error)
        "one level deeper signals cache-error")))
