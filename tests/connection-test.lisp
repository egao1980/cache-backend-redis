(in-package #:cache-backend-redis/tests)

;;; Connection management without a live server: a scripted RESP server
;;; on an ephemeral port (usocket + this library's codec + FAKE-REDIS)
;;; records what the backend sends. Also redis:// URLs, error replies
;;; (#4) and CAS :ttl / expected NIL (#5).

(defstruct (scripted-server (:conc-name ss-))
  listener port thread (stopping nil) (log '())
  (lock (bt2:make-lock :name "scripted-server")))

(defun %serve-connection (server sock dispatch conn-id drop-after)
  (let ((stream (usocket:socket-stream sock))
        (served 0))
    (unwind-protect
         (handler-case
             (loop
               (let ((parts (decode-resp3 stream)))
                 (bt2:with-lock-held ((ss-lock server))
                   (push (cons conn-id parts) (ss-log server)))
                 (encode-resp3 (funcall dispatch parts) stream)
                 (force-output stream)
                 (incf served)
                 (when (and drop-after (= conn-id 1) (>= served drop-after))
                   (return))))
           ;; The client went away: the scripted server just stops serving it.
           ((or cache-protocol:cache-error stream-error usocket:socket-error) () nil))
      (usocket:socket-close sock))))

(defun start-scripted-server (dispatch &key drop-after)
  "Serve DISPATCH (command list → reply) on 127.0.0.1, one connection at
a time. DROP-AFTER closes the first connection after that many replies."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                        :element-type '(unsigned-byte 8)))
         (server (make-scripted-server :listener listener
                                       :port (usocket:get-local-port listener))))
    (setf (ss-thread server)
          (bt2:make-thread
           (lambda ()
             ;; Poll the listener so STOP-SCRIPTED-SERVER never has to
             ;; interrupt a blocked accept().
             (loop with conn-id = 0
                   until (ss-stopping server)
                   when (usocket:wait-for-input listener :timeout 0.05 :ready-only t)
                     do (%serve-connection server (usocket:socket-accept listener)
                                           dispatch (incf conn-id) drop-after)))
           :name "scripted-redis"))
    server))

(defun stop-scripted-server (server)
  (setf (ss-stopping server) t)
  (bt2:join-thread (ss-thread server))
  (usocket:socket-close (ss-listener server))
  server)

(defun ss-commands (server &optional conn-id)
  "Commands received, oldest first, as (conn-id . parts) or, given CONN-ID, parts."
  (let ((log (reverse (bt2:with-lock-held ((ss-lock server)) (ss-log server)))))
    (if conn-id
        (mapcar #'cdr (remove conn-id log :key #'car :test-not #'eql))
        log)))

(defmacro with-scripted-server ((var dispatch &rest keys) &body body)
  `(let ((,var (start-scripted-server ,dispatch ,@keys)))
     (unwind-protect (progn ,@body)
       (stop-scripted-server ,var))))

(defun %fake-dispatch (&rest fake-args)
  (let ((fake (apply #'make-fake-redis fake-args)))
    (lambda (parts) (fake-redis-dispatch fake parts))))

(defun %wait-until (fn &optional (seconds 2))
  (loop repeat (* seconds 100)
        when (funcall fn) return t
        do (sleep 0.01)))

(deftest handshake-sends-hello-auth-setname-select
  (with-scripted-server (srv (%fake-dispatch :password "pw"))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv)
                         :password "pw" :db 3 :client-name "unit")
      (ok (not (redis-cache-backend-connected-p c)) "connects lazily")
      (cache-protocol:cache-put c "k" "v")
      (ok (redis-cache-backend-connected-p c))
      (ok (equal "v" (cache-protocol:cache-get c "k"))))
    (ok (equal '(("HELLO" "3" "AUTH" "default" "pw" "SETNAME" "unit")
                 ("SELECT" "3")
                 ("SET" "k" "v")
                 ("GET" "k"))
               (ss-commands srv 1))
        (format nil "wire: ~S" (ss-commands srv 1)))))

(deftest handshake-without-password-or-db
  (with-scripted-server (srv (%fake-dispatch))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :username "app")
      (cache-protocol:cache-get c "k"))
    (ok (equal '(("HELLO" "3") ("GET" "k")) (ss-commands srv 1))
        "no AUTH without a password, no SELECT for db 0")))

(deftest handshake-falls-back-without-hello
  ;; Redis 5: no HELLO. The backend authenticates with AUTH and stays on RESP2.
  (let ((fake (make-fake-redis :password "pw")))
    (with-scripted-server (srv (lambda (parts)
                                 (if (string-equal "HELLO" (first parts))
                                     (make-resp-error "ERR unknown command 'HELLO'")
                                     (fake-redis-dispatch fake parts))))
      (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv)
                           :username "u" :password "pw" :db 1)
        (cache-protocol:cache-put c "k" "v"))
      (ok (equal '(("HELLO" "3" "AUTH" "u" "pw") ("AUTH" "u" "pw")
                   ("SELECT" "1") ("SET" "k" "v"))
                 (ss-commands srv 1))
          (format nil "wire: ~S" (ss-commands srv 1))))))

(deftest wrong-password-signals-cache-error
  (with-scripted-server (srv (%fake-dispatch :password "pw"))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :password "nope")
      (let ((e (handler-case (progn (cache-protocol:cache-get c "k") nil)
                 (cache-protocol:cache-error (e) e))))
        (ok (typep e 'cache-protocol:cache-error) "refused AUTH signals cache-error")
        (ok (not (typep e 'cache-protocol:cache-connection-error))
            "a refusal is not a transport failure (no reconnect)")
        (ok (and e (search "WRONGPASS" (princ-to-string e))) "carries the server text")
        (ok (not (redis-cache-backend-connected-p c)) "the half-open socket is dropped")))
    (ok (= 1 (length (ss-commands srv))) "HELLO sent once, not retried")))

(deftest reconnects-after-the-server-drops-the-socket
  ;; The first connection is closed after HELLO, SELECT and SET.
  (with-scripted-server (srv (%fake-dispatch) :drop-after 3)
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :db 2)
      (cache-protocol:cache-put c "k" "v")
      (%wait-until (lambda () (= 3 (length (ss-commands srv 1)))))
      (sleep 0.05)
      (ok (equal "v" (cache-protocol:cache-get c "k")) "GET after the drop succeeds"))
    (ok (equal '(("HELLO" "3") ("SELECT" "2") ("GET" "k")) (ss-commands srv 2))
        (format nil "second connection redoes the handshake: ~S" (ss-commands srv 2)))))

(deftest no-retry-leaves-a-reconnect-restart
  (with-scripted-server (srv (%fake-dispatch) :drop-after 2)
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :retries 0)
      (cache-protocol:cache-put c "k" "v")
      (%wait-until (lambda () (= 2 (length (ss-commands srv 1)))))
      (sleep 0.05)
      (ok (signals (cache-protocol:cache-get c "k") 'cache-protocol:cache-connection-error)
          ":retries 0 surfaces the dropped socket")
      (cache-protocol:cache-put c "k" "again")
      (ok (equal "again" (cache-protocol:cache-get c "k")) "the next call reconnects"))
    (with-scripted-server (srv (%fake-dispatch) :drop-after 2)
      (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :retries 0)
        (cache-protocol:cache-put c "k" "v")
        (%wait-until (lambda () (= 2 (length (ss-commands srv 1)))))
        (sleep 0.05)
        (let ((got (handler-bind ((cache-protocol:cache-connection-error
                                    (lambda (e)
                                      (invoke-restart (find-restart 'reconnect e)))))
                     (cache-protocol:cache-get c "k"))))
          (ok (equal "v" got) "a handler can invoke RECONNECT"))))))

(deftest refused-connect-signals-connection-error-with-cause
  (let ((c (make-redis-cache-backend :host "127.0.0.1" :port 1)))
    (let ((e (handler-case (progn (cache-protocol:cache-get c "k") nil)
               (cache-protocol:cache-connection-error (e) e))))
      (ok e "cache-connection-error")
      (ok (and e (typep (cache-protocol:cache-connection-error-cause e)
                        'usocket:socket-error))
          "the usocket condition is kept as CAUSE")
      (ok (and e (search "127.0.0.1:1" (princ-to-string e))) "the report names the endpoint"))))

(deftest close-releases-and-reports
  (with-scripted-server (srv (%fake-dispatch))
    (let ((c (make-redis-cache-backend :host "127.0.0.1" :port (ss-port srv))))
      (ok (null (close c)) "CLOSE before connecting → NIL")
      (cache-protocol:cache-put c "k" "v")
      (ok (eq t (close c)) "CLOSE of an open connection → T")
      (ok (not (redis-cache-backend-connected-p c)))
      (ok (equal "v" (cache-protocol:cache-get c "k")) "used again, it reconnects")
      (close c))
    (ok (= 2 (count "HELLO" (ss-commands srv) :key #'second :test #'equal))
        "two connections")))

(deftest caller-stream-is-not-closed
  (let* ((io (make-two-way-stream (flexi-streams:make-in-memory-input-stream #())
                                  (flexi-streams:make-in-memory-output-stream)))
         (c (make-redis-cache-backend :stream io)))
    (ok (null (close c)))
    (ok (open-stream-p io) "the caller's :stream stays open")
    (ok (signals (cache-protocol:cache-get c "k") 'cache-protocol:cache-connection-error)
        "EOF on a caller stream is a connection error, not retried")))

;;; --- URLs ---------------------------------------------------------------

(deftest-parametrize parse-redis-urls
    ((url expected)
     ("redis://localhost" '(:host "localhost" :port 6379 :username nil :password nil
                            :db 0 :tls nil))
     ("redis://:s3cret@10.0.0.5:16390/6" '(:host "10.0.0.5" :port 16390 :username nil
                                           :password "s3cret" :db 6 :tls nil))
     ("rediss://app:p%40ss@cache.example.com:6380/0"
      '(:host "cache.example.com" :port 6380 :username "app" :password "p@ss"
        :db 0 :tls t))
     ("REDIS://h:1/" '(:host "h" :port 1 :username nil :password nil :db 0 :tls nil)))
  (ok (equal expected (parse-redis-url url))
      (format nil "~A → ~S" url (parse-redis-url url))))

(deftest-parametrize bad-redis-urls
    ((url)
     ("http://localhost") ("redis://h/x") ("redis://h/-1") ("redis:///0"))
  (ok (signals (parse-redis-url url) 'cache-protocol:cache-error) url))

(deftest url-and-explicit-keywords
  (let ((c (make-redis-cache-backend :url "rediss://u:p@h:7000/4" :db 9)))
    (ok (equal "h" (redis-cache-backend-host c)))
    (ok (= 7000 (redis-cache-backend-port c)))
    (ok (equal "u" (redis-cache-backend-username c)))
    (ok (= 9 (redis-cache-backend-db c)) "an explicit :db wins over the URL path")
    (ok (redis-cache-backend-tls c) "rediss:// means TLS")
    (ok (not (search "@" (princ-to-string c)))
        (format nil "the printed backend carries no userinfo: ~A" c))))

;;; --- error replies (#4), TTL arguments, CAS (#5) --------------------------

(defun %recording-cache (&rest fake-args)
  "→ (values backend fake commands-fn)."
  (let* ((fake (apply #'make-fake-redis fake-args))
         (sent '())
         (c (make-redis-cache-backend
             :send-fn (lambda (parts)
                        (push parts sent)
                        (fake-redis-dispatch fake parts)))))
    (values c fake (lambda () (reverse sent)))))

(deftest put-error-reply-signals
  (let ((c (%recording-cache)))
    (let ((e (handler-case (progn (cache-protocol:cache-put c "k" "v" :ttl 0) nil)
               (cache-protocol:cache-error (e) e))))
      (ok e "SET ... EX 0 refused by the server signals cache-error")
      (ok (and e (search "invalid expire time" (princ-to-string e))))
      (ok (and e (equal "k" (cache-protocol:cache-error-key e))) "with the key"))
    (ok (signals (cache-protocol:cache-put c "k" "v" :ttl -1.5)
                 'cache-protocol:cache-error))))

(deftest incr-error-reply-signals
  (let ((c (%recording-cache)))
    (cache-protocol:cache-put c "k" "abc")
    (let ((e (handler-case (progn (cache-protocol:cache-incr c "k") nil)
               (cache-protocol:cache-error (e) e))))
      (ok e "INCR of a non-integer signals cache-error")
      (ok (and e (search "not an integer" (princ-to-string e)))))))

(deftest-parametrize ttl-wire-arguments
    ((ttl args)
     (10 '("EX" "10"))
     (1.5 '("PX" "1500"))
     (1/4 '("PX" "250"))
     (0.0001 '("PX" "1")))
  (multiple-value-bind (c fake sent) (%recording-cache)
    (declare (ignore fake))
    (cache-protocol:cache-put c "k" "v" :ttl ttl)
    (ok (equal (append '("SET" "k" "v") args) (first (funcall sent)))
        (format nil ":ttl ~S → ~S" ttl args))))

(deftest fractional-ttl-expires
  (let* ((now 100)
         (fake (make-fake-redis :clock (lambda () now)))
         (c (make-redis-cache-backend :send-fn (fake-redis-send-fn fake))))
    (cache-protocol:cache-put c "k" "v" :ttl 1/2)
    (setf now (+ 100 1/4))
    (ok (equal "v" (cache-protocol:cache-get c "k")))
    (setf now (+ 100 1/2))
    (ok (null (cache-protocol:cache-get c "k")))))

(deftest cas-ttl-and-absent
  (let* ((now 100)
         (fake (make-fake-redis :clock (lambda () now)))
         (sent '())
         (c (make-redis-cache-backend
             :send-fn (lambda (parts) (push parts sent) (fake-redis-dispatch fake parts)))))
    (ok (equal "first" (cache-protocol:cache-cas c "k" nil "first"))
        "expected NIL creates a missing key")
    (ok (signals (cache-protocol:cache-cas c "k" nil "again")
                 'cache-protocol:cache-cas-mismatch)
        "expected NIL does not match a present key")
    (ok (equal "next" (cache-protocol:cache-cas c "k" "first" "next" :ttl 5)))
    (ok (equal '("1" "k" "0" "first" "next" "5000")
               (subseq (find "EVAL" sent :key #'first :test #'equal) 2))
        "the script gets the absent flag and PX milliseconds")
    (setf now 105)
    (ok (null (cache-protocol:cache-get c "k")) "CAS :ttl expires the new value")
    (ok (signals (cache-protocol:cache-cas c "k" "x" "y" :ttl 0)
                 'cache-protocol:cache-error))))

(deftest cas-lua-script-shape
  (ok (search "v ~= false" +cas-lua+) "the script tests a missing key with false, not \"\"")
  (ok (search "'PX'" +cas-lua+) "the script sets an expiry"))
