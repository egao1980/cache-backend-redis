(in-package #:cache-backend-redis/tests)

;;; Adversarial review b2-adv1 of PR #6 (connect, AUTH / SELECT, reconnect).
;;; Each test below fails on agent/redis-connect-24a9 (b06d938).
;;;
;;; Reference: redis-py 8.2.0, run 2026-11-24 against scripted servers and
;;; the containers below. Live tests skip without their server:
;;;   CACHE_REDIS_TEST_HOST / _PORT / _PASSWORD   Valkey 8, --requirepass (connect-test.lisp)
;;;   CACHE_REDIS_TEST_REDIS5_PORT    redis:5 with the same --requirepass
;;;   CACHE_REDIS_TEST_REDIS7_PORT    redis:7 with the same --requirepass
;;;   CACHE_REDIS_TEST_TLS_WRONG_NAME_PORT   Valkey 8 TLS whose certificate, signed by
;;;                                   CACHE_REDIS_TEST_CA, names only DNS:other.example

(defun %env-port (name)
  (let ((v (uiop:getenv name)))
    (and v (plusp (length v)) (parse-integer v))))

(defun %side-open-port (port)
  "An authenticated side connection to the live host on PORT, or NIL."
  (let ((sock (handler-case
                  (usocket:socket-connect (%live-host) port
                                          :element-type '(unsigned-byte 8)
                                          :timeout 2)
                (error () nil))))
    (when sock
      (%side sock "AUTH" (%live-password))
      sock)))

(defun %wire (&rest lines)
  "LINES joined by CRLF (with a final CRLF), as ASCII octets."
  (map '(vector (unsigned-byte 8)) #'char-code
       (format nil "~{~A~C~C~}"
               (loop for l in lines append (list l #\Return #\Linefeed)))))

(defun %show-wire (octets)
  "OCTETS as text, CR and LF shown as \\r \\n."
  (with-output-to-string (out)
    (loop for b across octets
          do (case b
               (13 (write-string "\\r" out))
               (10 (write-string "\\n" out))
               (t (write-char (code-char b) out))))))

(defun %start-raw-server (handler)
  "A server on 127.0.0.1, one connection at a time. HANDLER gets (conn-id
parts stream), writes its own reply octets, and returns :CLOSE to drop the
connection."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                        :element-type '(unsigned-byte 8)))
         (server (make-scripted-server :listener listener
                                       :port (usocket:get-local-port listener))))
    (setf (ss-thread server)
          (bt2:make-thread
           (lambda ()
             (loop with conn-id = 0
                   until (ss-stopping server)
                   when (usocket:wait-for-input listener :timeout 0.05 :ready-only t)
                     do (let* ((sock (usocket:socket-accept listener))
                               (stream (usocket:socket-stream sock))
                               (id (incf conn-id)))
                          (unwind-protect
                               (handler-case
                                   (loop
                                     (let ((parts (decode-resp3 stream)))
                                       (bt2:with-lock-held ((ss-lock server))
                                         (push (cons id parts) (ss-log server)))
                                       (when (eq :close (funcall handler id parts stream))
                                         (return))
                                       (force-output stream)))
                                 ((or cache-protocol:cache-error stream-error usocket:socket-error)
                                   ()
                                   nil))
                            (usocket:socket-close sock)))))
           :name "raw-redis"))
    server))

(defmacro with-raw-server ((var handler) &body body)
  `(let ((,var (%start-raw-server ,handler)))
     (unwind-protect (progn ,@body)
       (stop-scripted-server ,var))))

(defun %hello-ok (stream)
  (encode-resp3 (make-resp-map (list (cons "server" "raw") (cons "proto" 3))) stream))

;;; --- 1. The caller's printer variables reach the wire ------------------------
;;;
;;; Bulk lengths (%WRITE-HEADER), SELECT db, EX / PX seconds, INCRBY deltas
;;; and CAS milliseconds are written with PRINC-TO-STRING, which follows the
;;; caller's *PRINT-BASE* and *PRINT-RADIX* (a REPL after (setf *print-base*
;;; 16), or code that prints hex around a cache call). ENCODE-RESP3 itself
;;; uses ~D for integers. redis-py writes decimal whatever the caller does.

(deftest printer-variables-do-not-change-command-arguments
  (multiple-value-bind (c fake sent) (%recording-cache)
    (declare (ignore fake))
    (let ((*print-base* 16))
      (cache-protocol:cache-put c "k" "v" :ttl 16)
      (cache-protocol:cache-incr c "n" :delta 16)
      (handler-case (cache-protocol:cache-cas c "k" "v" "w" :ttl 16)
        (cache-protocol:cache-error () nil)))
    (destructuring-bind (put incr cas &rest more) (funcall sent)
      (declare (ignore more))
      (ok (equal '("SET" "k" "v" "EX" "16") put)
          (format nil ":ttl 16 under *print-base* 16 → EX 16 (sent ~S)" put))
      (ok (equal '("INCRBY" "n" "16") incr)
          (format nil ":delta 16 → INCRBY n 16 (sent ~S)" incr))
      (ok (equal "16000" (car (last cas)))
          (format nil "CAS :ttl 16 → PX 16000 (sent ~S)" (car (last cas)))))))

(deftest printer-variables-do-not-change-the-database
  (with-scripted-server (srv (%fake-dispatch))
    (let ((*print-base* 16))
      (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :db 16)
        (cache-protocol:cache-get c "k")))
    (ok (member '("SELECT" "16") (ss-commands srv 1) :test #'equal)
        (format nil ":db 16 under *print-base* 16 → SELECT 16 (wire ~S)" (ss-commands srv 1)))))

(deftest-parametrize printer-variables-do-not-change-bulk-lengths
    ((variable value)
     ('*print-base* 16)
     ('*print-radix* t))
  (let* ((parts (list "SET" "k" (make-string 16 :initial-element #\a)))
         (plain (encode-resp3-command parts))
         (got (progv (list variable) (list value) (encode-resp3-command parts))))
    (ok (equalp plain got)
        (format nil "~A ~A: ~A (decimal: ~A)" variable value (%show-wire got) (%show-wire plain)))))

(defun %injection-value (inj-key)
  "A 144-character value. Its bulk length written with *PRINT-BASE* 16 is
$90: a server reads 90 octets as the value, skips two, and runs the other
52 as an inline command, SET INJ-KEY 111..."
  (let ((inj (format nil "SET ~A " inj-key)))
    (concatenate 'string (make-string 90 :initial-element #\a) "xx"
                 inj (make-string (- 52 (length inj)) :initial-element #\1))))

(deftest-parametrize a-cached-value-cannot-run-a-command
    ((server)
     (:live)
     (:redis7))
  ;; README: "a value containing CRLF cannot start a second command". Under
  ;; *print-base* 16 no CRLF is needed. Redis 7.4.9 and 5.0.14 run the
  ;; injected SET, and the next GET reads its +OK; Valkey 8 refuses the
  ;; request ("Protocol error: invalid CRLF in request").
  (let* ((port (if (eq server :live) (%live-port) (%env-port "CACHE_REDIS_TEST_REDIS7_PORT")))
         (side (and port (%side-open-port port))))
    (if (null side)
        (skip (format nil "no ~A server (see the file header)" server))
        (unwind-protect
             (let* ((key (format nil "b2adv1:inj:~D" (random 1000000)))
                    (value (%injection-value key))
                    (got (handler-case
                             (with-redis-cache (c :host (%live-host) :port port
                                                  :password (%live-password))
                               (let ((*print-base* 16))
                                 (cache-protocol:cache-put c "b2a:val" value))
                               (cache-protocol:cache-get c "b2a:val"))
                           (cache-protocol:cache-error (e) (princ-to-string e))))
                    (injected (%side side "GET" key)))
               (ok (equal value got)
                   (format nil "~A: put then get under *print-base* 16 round-trips (got ~S)"
                           server (if (and (stringp got) (> (length got) 60))
                                      (concatenate 'string (subseq got 0 60) "...")
                                      got)))
               (ok (eq :resp-null injected)
                   (format nil "~A: nothing inside the value ran as a command (GET ~A → ~S)"
                           server key injected))
               (%side side "DEL" key "b2a:val"))
          (usocket:socket-close side)))))

;;; --- 2. RESP3 push frames are taken as the reply ------------------------------
;;;
;;; RESP3 spec: push data (">") is out-of-band, not the reply to a command; a
;;; client handles it and reads on (redis-py's RESP3 parser does). DECODE-RESP3
;;; returns it as an array, so the GET returns the push and leaves its own
;;; reply on the socket for the next command: every reply after it is one late.
;;; redis-py 8.2.0, same server: GET k → 'v', then GET other → 'other'.

(deftest-parametrize skip-push-keeps-every-other-reply
    ((wire expected)
     (":3" 3)
     ("(12345678901234567890" 12345678901234567890)
     ("+OK" "OK")
     ("*1" '("a"))
     (">2" '("x" "y")))
  ;; A reply that is a push frame is followed here by "$1 z": the push is
  ;; skipped and z returned. Anything else is the reply itself.
  (let* ((octets (if (char= #\> (char wire 0))
                     (%wire wire "$1" "x" "$1" "y" "$1" "z")
                     (%wire wire "$1" "a")))
         (got (decode-resp3 (flexi-streams:make-in-memory-input-stream octets) :skip-push t))
         (shown (if (resp-simple-string-p got) (resp-simple-string-value got) got)))
    (ok (equal (if (char= #\> (char wire 0)) "z" expected) shown)
        (format nil "~A with :skip-push → ~S (got ~S)" wire expected shown))))

(deftest a-push-frame-is-not-the-reply
  (with-raw-server (srv (lambda (id parts s)
                          (declare (ignore id))
                          (cond ((string-equal "HELLO" (first parts)) (%hello-ok s))
                                ((equal parts '("GET" "k"))
                                 ;; a client-tracking invalidation, then GET's reply
                                 (write-sequence (%wire ">2" "$10" "invalidate" "*1" "$1" "k"
                                                        "$1" "v")
                                                 s))
                                ((equal parts '("GET" "other"))
                                 (write-sequence (%wire "$5" "other") s))
                                (t (encode-resp3 (make-resp-simple-string "OK") s)))
                          nil))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv))
      (let* ((first-get (handler-case (cache-protocol:cache-get c "k") (error (e) e)))
             (second-get (handler-case (cache-protocol:cache-get c "other") (error (e) e))))
        (ok (equal "v" first-get) (format nil "GET k after a push frame → \"v\" (got ~S)" first-get))
        (ok (equal "other" second-get)
            (format nil "the next GET gets its own reply (got ~S)" second-get))))))

;;; --- 3. Redis 5: AUTH user pass is not retried as AUTH pass -------------------
;;;
;;; Redis 5 has no HELLO and no ACL: AUTH takes one argument. The fallback
;;; sends AUTH user pass whenever a username is given, and Redis 5 answers
;;; "ERR wrong number of arguments for 'auth' command". redis-py retries with
;;; AUTH pass (AuthenticationWrongNumberOfArgsError) and connects: 8.2.0
;;; against redis:5.0.14, Redis(username="default", password=...) and
;;; from_url("redis://default:...@host/0") both work. Cloud providers hand
;;; out redis://default:password@host URLs.

(defun %redis5-dispatch (fake password)
  (lambda (parts)
    (cond ((string-equal "HELLO" (first parts))
           (make-resp-error "ERR unknown command `HELLO`, with args beginning with: "))
          ((string-equal "AUTH" (first parts))
           (cond ((/= 2 (length parts))
                  (make-resp-error "ERR wrong number of arguments for 'auth' command"))
                 ((equal password (second parts)) (make-resp-simple-string "OK"))
                 (t (make-resp-error "ERR invalid password"))))
          (t (fake-redis-dispatch fake parts)))))

(deftest redis5-auth-with-a-username-falls-back-to-the-password
  (with-scripted-server (srv (%redis5-dispatch (make-fake-redis) "pw"))
    (let ((got (handler-case
                   (with-redis-cache (c :url (format nil "redis://default:pw@127.0.0.1:~D/0"
                                                     (ss-port srv)))
                     (cache-protocol:cache-put c "k" "v")
                     (cache-protocol:cache-get c "k"))
                 (cache-protocol:cache-error (e) (princ-to-string e)))))
      (ok (equal "v" got)
          (format nil "redis://default:***@ to a Redis 5 → \"v\" (got ~S; wire ~S)"
                  got (mapcar #'cdr (ss-commands srv)))))))

(deftest redis5-auth-retry-sends-the-password-alone
  (with-scripted-server (srv (%redis5-dispatch (make-fake-redis) "pw"))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :username "default" :password "pw")
      (cache-protocol:cache-get c "k"))
    (ok (equal '(("HELLO" "3" "AUTH" "default" "pw") ("AUTH" "default" "pw") ("AUTH" "pw")
                 ("GET" "k"))
               (ss-commands srv 1))
        (format nil "wire: ~S" (ss-commands srv 1)))))

(deftest redis5-username-without-a-password-sends-no-auth
  ;; No HELLO and no password: nothing to authenticate with on Redis 5.
  (with-scripted-server (srv (%redis5-dispatch (make-fake-redis) "pw"))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :username "app")
      (cache-protocol:cache-get c "k"))
    (ok (equal '(("HELLO" "3" "AUTH" "app" "") ("GET" "k")) (ss-commands srv 1))
        (format nil "wire: ~S" (ss-commands srv 1)))))

(deftest redis5-wrong-password-is-still-refused
  (with-scripted-server (srv (%redis5-dispatch (make-fake-redis) "pw"))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :username "default" :password "no")
      (let ((e (handler-case (progn (cache-protocol:cache-get c "k") nil)
                 (cache-protocol:cache-error (e) e))))
        (ok (and e (search "invalid password" (princ-to-string e)))
            (format nil "AUTH pass refused → cache-error (got ~A)" e))))))

(deftest live-redis5-with-a-username
  (let ((port (%env-port "CACHE_REDIS_TEST_REDIS5_PORT")))
    (if (null port)
        (skip "no Redis 5 (CACHE_REDIS_TEST_REDIS5_PORT)")
        (let ((got (handler-case
                       (with-redis-cache (c :host (%live-host) :port port
                                            :username "default" :password (%live-password))
                         (cache-protocol:cache-put c "b2adv1-r5" "v")
                         (cache-protocol:cache-get c "b2adv1-r5"))
                     (cache-protocol:cache-error (e) (princ-to-string e)))))
          (ok (equal "v" got)
              (format nil "Redis 5, :username \"default\" :password → \"v\" (got ~S)" got))))))

;;; --- 4. A username without a password is dropped --------------------------------
;;;
;;; :username "app" with no :password sends a bare HELLO 3, so the backend
;;; runs as the default user: NOAUTH when default needs a password, or with
;;; default's permissions when it does not. redis-py 8.2.0 sends
;;; HELLO 3 AUTH app "" and, against Valkey 8 with an ACL nopass user,
;;; Redis(username=...) and from_url("redis://user@host/0") both run as that
;;; user (ACL WHOAMI). (handshake-without-password-or-db asserts the bare HELLO.)

(deftest a-username-without-a-password-logs-in-as-that-user
  (with-side (side)
    (let ((user (format nil "b2adv1-~D" (random 1000000))))
      (%side side "ACL" "SETUSER" user "on" "nopass" "~*" "+@all")
      (unwind-protect
           (let ((got (handler-case
                          (with-redis-cache (c :host (%live-host) :port (%live-port) :username user)
                            (cache-protocol:cache-put c "b2adv1-user" "v")
                            (cache-protocol:cache-get c "b2adv1-user"))
                        (cache-protocol:cache-error (e) (princ-to-string e)))))
             (ok (equal "v" got)
                 (format nil ":username of a nopass ACL user, no :password → \"v\" (got ~S)" got)))
        (%side side "ACL" "DELUSER" user)
        (%side side "DEL" "b2adv1-user")))))

;;; --- 5. -LOADING after a (re)connect is not retried --------------------------
;;;
;;; A Redis restarting with a dataset accepts connections and answers
;;; "-LOADING Redis is loading the dataset in memory" until it is loaded: the
;;; restart the README says the retries ride out. redis-py maps LOADING to
;;; BusyLoadingError, a ConnectionError, so its Retry reconnects and resends
;;; (8.2.0, scripted server: wire HELLO, GET / HELLO, GET → 'v').

(deftest a-loading-reply-is-retried-like-a-dropped-connection
  (let ((fake (make-fake-redis))
        (gets 0))
    (fake-redis-dispatch fake '("SET" "k" "v"))
    (with-scripted-server (srv (lambda (parts)
                                 (if (and (string-equal "GET" (first parts)) (= 1 (incf gets)))
                                     (make-resp-error "LOADING Redis is loading the dataset in memory")
                                     (fake-redis-dispatch fake parts))))
      (let ((got (handler-case
                     (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :retry-delay 0)
                       (cache-protocol:cache-get c "k"))
                   (cache-protocol:cache-error (e) (princ-to-string e)))))
        (ok (equal "v" got)
            (format nil "GET answered -LOADING once → \"v\" after a resend (got ~S)" got))))))

;;; --- 6. A TLS hostname mismatch escapes as a raw cl+ssl condition ------------
;;;
;;; README: "TLS failure → cache-connection-error, the cl+ssl condition in
;;; cause". cl+ssl's HOSTNAME-VERIFICATION-ERROR (UNABLE-TO-MATCH-ALTNAMES,
;;; -COMMON-NAME) inherits from ERROR, not CL+SSL-ERROR, so
;;; %WITH-TRANSPORT-ERRORS lets it through: a handler for CACHE-ERROR never
;;; sees it. (An untrusted CA, SSL-ERROR-VERIFY, is mapped.)

(deftest tls-hostname-mismatch-is-a-cache-connection-error
  (let ((port (%env-port "CACHE_REDIS_TEST_TLS_WRONG_NAME_PORT")))
    (if (not (and port (%trust-test-ca)))
        (skip "CACHE_REDIS_TEST_TLS_WRONG_NAME_PORT / CACHE_REDIS_TEST_CA not set")
        (let ((got (%call-with-limit 5 (lambda ()
                                         (with-redis-cache (c :host "127.0.0.1" :port port :tls t
                                                              :password (%live-password))
                                           (cache-protocol:cache-get c "k"))))))
          (ok (typep got 'cache-protocol:cache-connection-error)
              (format nil "a certificate for other.example at 127.0.0.1 → cache-connection-error ~
                           (got ~S: ~A)"
                      (type-of got) got))))))

;;; --- 7. A write to a server that stops reading hangs ----------------------------
;;;
;;; :connect-timeout bounds "each command" (README). Over plain TCP the
;;; socket is blocking and SBCL checks the deadline only while it waits for
;;; input, so a command larger than the socket buffers blocks in write() for
;;; as long as the server reads nothing: a frozen server, a partition, a long
;;; script. TLS is bounded (cl+ssl waits with the deadline). redis-py 8.2.0,
;;; socket_timeout=0.5, same deaf server: "TimeoutError: Timeout writing to
;;; socket" after 0.51 s.

(defun %call-with-deaf-server (fn)
  "FN of the port of a server on 127.0.0.1 that answers HELLO, then reads nothing."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                        :element-type '(unsigned-byte 8)))
         (accepted '())
         (stopping nil)
         (acceptor (bt2:make-thread
                    (lambda ()
                      (loop until stopping
                            when (usocket:wait-for-input listener :timeout 0.05 :ready-only t)
                              do (let* ((sock (usocket:socket-accept listener))
                                        (s (usocket:socket-stream sock)))
                                   (push sock accepted)
                                   (handler-case (progn (decode-resp3 s) (%hello-ok s) (force-output s))
                                     ((or cache-protocol:cache-error stream-error) () nil)))))
                    :name "deaf-redis")))
    (unwind-protect (funcall fn (usocket:get-local-port listener))
      (setf stopping t)
      (bt2:join-thread acceptor)
      (mapc #'usocket:socket-close accepted)
      (usocket:socket-close listener))))

(deftest a-server-that-stops-reading-is-a-connection-error-not-a-hang
  (if (uiop:os-windows-p)
      (skip "Windows, TCP: SBCL reads a socket with no timeout (cache-backend-redis#9)")
      (%call-with-deaf-server
       (lambda (port)
         (let ((got (%call-with-limit 5 (lambda ()
                                          (with-redis-cache (c :host "127.0.0.1" :port port
                                                               :connect-timeout 1/2 :retries 0)
                                            (cache-protocol:cache-put
                                             c "k" (make-string (* 16 1024 1024)
                                                                :initial-element #\a)))))))
           (ok (typep got 'cache-protocol:cache-connection-error)
               (format nil "a 16 MB put to a server that reads nothing, :connect-timeout 1/2 → ~
                            connection error within 5 s (got ~A)"
                       got)))))))

;;; --- 8. The password is in the backtrace of a failed handshake ----------------
;;;
;;; %ROUNDTRIP takes the command as a list, so while a transport error in
;;; HELLO is signalled its frame shows ("HELLO" "3" "AUTH" "default" <password>).
;;; SBCL run with --disable-debugger prints that backtrace to stderr for an
;;; unhandled error, and the debugger and error loggers show it too. Triggers:
;;; a server that closes on connect (maxclients, a proxy, TCP to a TLS port),
;;; a timeout in the handshake. Python tracebacks carry no argument values.

(defun %backtrace-text ()
  "This thread's backtrace as text, or NIL where it cannot be had."
  #+sbcl (with-output-to-string (s) (sb-debug:print-backtrace :stream s :count 60))
  #-sbcl nil)

(deftest a-failed-handshake-leaves-no-password-in-the-backtrace
  (let ((srv (start-flaky-server (%fake-dispatch :password "b2adv1-PASSWORD") '(0)))
        (backtrace nil))
    (unwind-protect
         (handler-case
             (handler-bind ((cache-protocol:cache-connection-error
                              (lambda (e)
                                (declare (ignore e))
                                (unless backtrace
                                  (setf backtrace (%backtrace-text))))))
               (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv)
                                    :password "b2adv1-PASSWORD")
                 (cache-protocol:cache-get c "k")))
           (cache-protocol:cache-error () nil))
      (stop-scripted-server srv))
    (if (null backtrace)
        (skip "no backtrace on this implementation")
        (ok (not (search "b2adv1-PASSWORD" backtrace))
            (format nil "no password in the backtrace; frames holding it:~%~{  ~A~%~}"
                    (remove-if-not (lambda (line) (search "b2adv1-PASSWORD" line))
                                   (uiop:split-string backtrace :separator '(#\Newline))))))))

;;; --- 9. URL errors show part of a password that holds / or ? ---------------
;;;
;;; %REDACT-URL: "This can over-mask a malformed URL, never under-mask it",
;;; and it masks "a port that is not a number (redis://:password with no
;;; @host)". With no @host, a password holding / or ? (base64 passwords hold
;;; /) is masked only up to that character.

(deftest-parametrize url-errors-mask-all-of-a-password-with-no-host
    ((url)
     ("redis://:pass/WORD-b2adv1")
     ("redis://app:pass/WORD-b2adv1")
     ("redis://:pass?WORD-b2adv1=1")
     ("redis://:pass#WORD-b2adv1")
     ;; all digits before the /, so it reads as a port after an empty host
     ("redis://:1234/WORD-b2adv1"))
  (let ((text (handler-case (progn (parse-redis-url url) "no error")
                (cache-protocol:cache-error (e) (princ-to-string e)))))
    (ok (string/= "no error" text) (format nil "~A is refused" url))
    (ok (not (search "WORD-b2adv1" text)) (format nil "no part of the password in: ~A" text))))

;;; --- 10. A caller's :stream is closed when :url or :host is given too ---------
;;;
;;; README: ":stream, an (unsigned-byte 8) stream the caller owns (never
;;; closed by the backend)". Given with :url (say REDIS_URL from the
;;; environment) the backend still uses the stream, but %MANAGED-P is true, so
;;; an error or CLOSE closes the caller's stream. Either keep it open or
;;; refuse the combination.

(deftest a-caller-stream-is-never-closed-even-next-to-a-url
  (let* ((io (make-two-way-stream (flexi-streams:make-in-memory-input-stream #())
                                  (flexi-streams:make-in-memory-output-stream)))
         (c (handler-case (make-redis-cache-backend :url "redis://127.0.0.1:1/0" :stream io
                                                    :connect-timeout 1/2)
              (error () :refused))))
    (unless (eq c :refused)
      (handler-case (cache-protocol:cache-get c "k")
        (cache-protocol:cache-error () nil)))
    (ok (or (eq c :refused) (open-stream-p io))
        ":url with :stream is refused, or the caller's stream stays open after an error")))

(deftest-parametrize a-caller-channel-is-refused-next-to-a-host-or-url
    ((args)
     ((list :url "redis://u:b2adv1-PW@127.0.0.1:1/0" :stream (make-two-way-stream
                                                              (make-string-input-stream "")
                                                              (make-string-output-stream))))
     ((list :host "127.0.0.1" :send-fn #'identity))
     ((list :url "redis://127.0.0.1/0" :send-fn #'identity)))
  (let ((e (handler-case (progn (apply #'make-redis-cache-backend args) nil)
             (cache-protocol:cache-error (e) e))))
    (ok (typep e 'cache-protocol:cache-error) (format nil "refused: ~A" e))
    (ok (not (and e (search "b2adv1-PW" (princ-to-string e)))) "no password in the report")))
