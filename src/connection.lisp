(in-package #:cache-backend-redis)

;;; Connection: a backend made with :host / :port (or :url) owns its
;;; socket. It connects on first use (usocket; TLS through cl-stack-ssl's
;;; cl+ssl), sends HELLO 3 with AUTH and SETNAME, then SELECT, and after a
;;; dropped socket reconnects and resends up to :RETRIES times (with
;;; backoff). CL:CLOSE releases it. A backend made with :stream or
;;; :send-fn uses that channel as is.

(defclass redis-cache-backend (cache-backend)
  ((stream :initarg :stream :accessor redis-cache-backend-stream :initform nil
           :documentation "(unsigned-byte 8) stream to the server: the caller's
:STREAM, or the managed connection's stream while connected.")
   (send-fn :initarg :send-fn :accessor redis-cache-backend-send-fn :initform nil)
   (host :initarg :host :reader redis-cache-backend-host :initform nil)
   (port :initarg :port :reader redis-cache-backend-port :initform 6379)
   (username :initarg :username :reader redis-cache-backend-username :initform nil)
   (password :initarg :password :initform nil)
   (db :initarg :db :reader redis-cache-backend-db :initform 0)
   (tls :initarg :tls :reader redis-cache-backend-tls :initform nil)
   (tls-verify :initarg :tls-verify :initform :required)
   (client-name :initarg :client-name :initform nil)
   (connect-timeout :initarg :connect-timeout :initform 5)
   (retries :initarg :retries :reader redis-cache-backend-retries :initform 1)
   (retry-delay :initarg :retry-delay :initform 1/10
                :documentation "Seconds before the second resend, doubled for
each later one (capped at 2 s). The first resend goes at once.")
   (socket :initform nil :accessor %socket)
   (ever-connected :initform nil :accessor %ever-connected
                   :documentation "True once a managed connection has completed its
handshake. From then on a refused reconnect is retried (see %CALL), in this
call and in every later one, for the life of the backend; CL:CLOSE keeps it.")
   (lock :initform (bt2:make-recursive-lock :name "redis-cache-backend")
         :reader %lock))
  (:documentation
   "cache-protocol backend over RESP3. With HOST it owns a connection (see
MAKE-REDIS-CACHE-BACKEND); with STREAM or SEND-FN it uses the caller's
channel. Commands are serialized by a lock, so one backend can be shared
between threads."))

(defmethod print-object ((backend redis-cache-backend) stream)
  (print-unreadable-object (backend stream :type t :identity t)
    (cond
     ((redis-cache-backend-host backend)
      (format stream "~:[redis~;rediss~]://~A:~D/~D~:[~; connected~]"
              (redis-cache-backend-tls backend)
              (%host-text (redis-cache-backend-host backend))
              (redis-cache-backend-port backend)
              (redis-cache-backend-db backend)
              (%socket backend)))
     ((redis-cache-backend-send-fn backend) (write-string "send-fn" stream))
     ((redis-cache-backend-stream backend) (write-string "stream" stream)))))

(defun %managed-p (backend)
  (and (redis-cache-backend-host backend) t))

(defun redis-cache-backend-connected-p (backend)
  "True while BACKEND holds an open connection it manages."
  (and (%socket backend) t))

;;; --- redis:// URLs -------------------------------------------------------

(defun %url-authority-start (url)
  "Index just past the :// in URL, or 0 without one."
  (let ((p (search "://" url)))
    (if p (+ p 3) 0)))

(defun %mask-query-item (item)
  "Query ITEM (name=value) for an error report: the value masked, except db's."
  (let ((eq (position #\= item)))
    (cond ((zerop (length item)) item)
          ((and eq (string= "db" item :end2 eq)) item)
          (eq (concatenate 'string (subseq item 0 (1+ eq)) "***"))
          (t "***"))))

(defun %userinfo-needs-escaping-p (url)
  "True when the text of URL between :// and its last @ holds a character
that a userinfo must percent-encode (RFC 3986 3.2.1 allows unreserved
characters, sub-delims, : and %XX), or a % without two hex digits: the
usual cause of a bad redis:// URL that carries a password."
  (let* ((start (%url-authority-start url))
         (at (position #\@ url :start start :from-end t)))
    (and at
         (loop for i from start below at
               for c = (char url i)
               thereis (if (char= c #\%)
                           (not (and (< (+ i 2) at)
                                     (digit-char-p (char url (+ i 1)) 16)
                                     (digit-char-p (char url (+ i 2)) 16)))
                           (not (or (and (char< c (code-char 128)) (alphanumericp c))
                                    (find c "-._~!$&'()*+,;=:"))))))))

(defun %url-host (raw)
  "The host of a URL as quri returns it (RAW), percent-decoded, as usocket
takes it: an IPv6 literal ([::1], [fe80::1%25en0]) without its brackets.
→ NIL when RAW is not an IPv6 literal or a host name (letters, digits,
- . _)."
  (flet ((decoded (text)
           (handler-case (encoding-protocol:percent-decode text)
             (encoding-protocol:encoding-error () nil)))
         (name-char-p (c)
           (or (and (char< c (code-char 128)) (alphanumericp c))
               (find c "-._~"))))
    (let ((n (length raw)))
      (if (and (plusp n) (char= #\[ (char raw 0)))
          (let* ((inner (and (> n 2) (char= #\] (char raw (1- n)))
                             (decoded (subseq raw 1 (1- n)))))
                 (zone (and inner (position #\% inner))))
            (and inner
                 (find #\: inner :end zone)
                 (every (lambda (c) (or (digit-char-p c 16) (find c ":.")))
                        (subseq inner 0 zone))
                 (or (null zone)
                     (and (< (1+ zone) (length inner))
                          (every #'name-char-p (subseq inner (1+ zone)))))
                 inner))
          (let ((name (decoded raw)))
            (and name (plusp (length name))
                 (every (lambda (c) (and (name-char-p c) (char/= c #\~))) name)
                 name))))))

(defun %host-text (host)
  "HOST as an endpoint or URL shows it: an IPv6 address in brackets."
  (if (find #\: host)
      (format nil "[~A]" host)
      host))

(defun %redact-url (url)
  "URL for an error report, with whatever may be a secret masked as ***:
URLs come from configuration, and reports end up in logs. Masked are the
password in the userinfo (all of the userinfo when it has no colon), a
port that is not a number (redis://:password with no @host), every query
value but db's (a redis-py URL may carry ?password=) and the fragment.
The userinfo is taken to end at the LAST @, not at the first / ? or #, so
a password that holds one of those unescaped (base64 passwords hold / + =)
is masked too. This can over-mask a malformed URL, never under-mask it."
  (let* ((start (%url-authority-start url))
         (at (position #\@ url :start start :from-end t))
         (colon (and at (position #\: url :start start :end at)))
         (host-start (if at (1+ at) start))
         (host-end (or (position-if (lambda (c) (find c "/?#")) url :start host-start)
                       (length url)))
         (bracket (and (< host-start host-end)
                       (char= #\[ (char url host-start))
                       (position #\] url :start host-start :end host-end)))
         (port-colon (position #\: url :start (or bracket host-start) :end host-end))
         (tail (subseq url host-end))
         (hash (position #\# tail))
         (query (position #\? tail :end hash)))
    (with-output-to-string (out)
      (write-string url out :end start)
      (cond (colon (write-string url out :start start :end (1+ colon))
                   (write-string "***@" out))
            (at (write-string "***@" out)))
      (cond ((and port-colon
                  (notevery #'digit-char-p (subseq url (1+ port-colon) host-end)))
             (write-string url out :start host-start :end (1+ port-colon))
             (write-string "***" out))
            (t (write-string url out :start host-start :end host-end)))
      (write-string tail out :end (or query hash (length tail)))
      (when query
        (format out "?~{~A~^&~}"
                (mapcar #'%mask-query-item
                        (uiop:split-string (subseq tail (1+ query) hash)
                                           :separator "&"))))
      (when hash
        (write-string "#***" out)))))

(defun %parse-db (text url)
  (handler-case (let ((n (parse-integer text)))
                  (check-type n (integer 0))
                  n)
    ((or parse-error type-error) ()
      (error 'cache-error
             :message (format nil "bad database number ~s in ~a" text url)))))

(defun parse-redis-url (url)
  "Parse redis://[[user]:password@]host[:port][/db][?db=N] (rediss:// for
TLS) the way redis-py's from_url reads it. → plist :host :port :username
:password :db :tls.
 - An IPv6 address is written in brackets (redis://[::1]:6379/0) and
   returned without them, as usocket takes it.
 - ?db=N wins over the path, as in redis-py (a blank one is ignored, the
   first one counts). Any other query parameter is refused rather than
   ignored. A fragment is ignored.
 - User, password, host, path and query are percent-decoded; / ? # @ and
   % in a user or password must be percent-encoded, as in any URL.
Errors are CACHE-ERRORs that print the URL with the password and the query
values masked (redis://user:***@host, see %REDACT-URL)."
  (check-type url string)
  (let ((shown (%redact-url url)))
    (labels ((refuse (what)
               ;; The escaping hint only when the user or password holds a
               ;; character that must be escaped: then it is the likely cause.
               (error 'cache-error
                      :message (format nil "~A: ~A~:[~; (escape % / ? # @ in the user or ~
                                            password as %25 %2F %3F %23 %40)~]"
                                       what shown (%userinfo-needs-escaping-p url))))
             (decode (text &optional plus-as-space)
               (handler-case (encoding-protocol:percent-decode text :plus-as-space plus-as-space)
                 (encoding-protocol:encoding-error ()
                   (refuse "bad percent-encoding in URL"))))
             (query-db (query)
               ;; redis-py: query values first; a blank db is ignored, the
               ;; first one counts, and an unknown name is refused (its
               ;; ConnectionPool rejects the keyword).
               (let ((db nil))
                 (loop for (name . value)
                         in (and query
                                 (handler-case (quri:url-decode-params query :percent-decode nil)
                                   (quri:uri-error () (refuse "malformed query in URL"))))
                       for key = (decode name t)
                       for text = (and value (string-trim " " (decode value t)))
                       do (cond ((string/= key "db")
                                 (refuse (if value
                                             (format nil "unsupported query parameter ~S in URL ~
                                                          (only db is read)"
                                                     key)
                                             "unsupported query parameter in URL (only db is read)")))
                                ((and (null db) (plusp (length text)))
                                 (setf db (%parse-db text shown)))))
                 db)))
      (let* ((uri (handler-case (quri:uri url)
                    (quri:uri-error () (refuse "malformed URL"))))
             (scheme (and (quri:uri-scheme uri)
                          (string-downcase (quri:uri-scheme uri)))))
        (unless (member scheme '("redis" "rediss") :test #'equal)
          (refuse "not a redis:// or rediss:// URL"))
        ;; An unescaped / ? or # in the password ends quri's authority
        ;; early, and the rest (with the @host) lands in the path, query or
        ;; fragment. Refuse that rather than connect to the wrong host.
        (when (some (lambda (part) (find #\@ (or part "")))
                    (list (quri:uri-path uri) (quri:uri-query uri) (quri:uri-fragment uri)))
          (refuse "malformed URL"))
        (when (zerop (length (quri:uri-host uri)))
          (refuse "no host in URL"))
        (let ((host (or (%url-host (quri:uri-host uri))
                        (refuse "bad host in URL")))
              (port (or (quri:uri-port uri) 6379)))
          (unless (typep port '(integer 1 65535))
            (refuse "bad port in URL"))
          (let* ((userinfo (quri:uri-userinfo uri))
                 (colon (and userinfo (position #\: userinfo)))
                 (user (and userinfo
                            (decode (subseq userinfo 0 (or colon (length userinfo))))))
                 (password (and colon (decode (subseq userinfo (1+ colon)))))
                 (path (string-trim "/" (decode (or (quri:uri-path uri) ""))))
                 (path-db (and (plusp (length path)) (%parse-db path shown)))
                 (query-db (query-db (quri:uri-query uri))))
            (list :host host
                  :port port
                  :username (and user (plusp (length user)) user)
                  :password (and password (plusp (length password)) password)
                  :db (or query-db path-db 0)
                  :tls (string= scheme "rediss"))))))))

(defun make-redis-cache-backend (&rest args
                                       &key stream send-fn url host port username
                                       password db tls (tls-verify :required)
                                       client-name (connect-timeout 5) (retries 1)
                                       (retry-delay 1/10))
  "A RESP3 backend. Either:
 - :HOST [:PORT 6379] [:USERNAME] [:PASSWORD] [:DB 0] [:TLS] or :URL
   \"redis://user:pass@host:port/db\" (\"rediss://\" for TLS; see
   PARSE-REDIS-URL): the backend owns a connection. It connects on first
   use, sends HELLO 3 (with AUTH and SETNAME), SELECT DB. Once it has had a
   connection, a dropped socket (or a refused reconnect) is retried on a
   new connection, up to RETRIES resends: the first at once, then after
   RETRY-DELAY seconds, doubled each time, capped at 2 s. CONNECT-TIMEOUT
   (seconds, NIL for none) bounds the connect, the TLS handshake and each
   command. Explicit keywords win over URL parts. TLS-VERIFY is cl+ssl's
   :VERIFY (:REQUIRED by default). CL:CLOSE or WITH-REDIS-CACHE releases
   the connection.
 - :STREAM, an (unsigned-byte 8) stream the caller owns, or
 - :SEND-FN, a function of the command list returning the decoded reply."
  (declare (ignore host port username password db tls))
  (let ((from-url (and url (parse-redis-url url))))
    (flet ((opt (key default)
             (let ((given (getf args key '%absent)))
               (if (eq given '%absent)
                   (getf from-url key default)
                   given))))
      (let ((host (opt :host nil))
            (port (opt :port 6379))
            (db (opt :db 0)))
        (check-type port (integer 0 65535))
        (check-type db (integer 0))
        (check-type retries (integer 0))
        (check-type retry-delay (real 0))
        (check-type connect-timeout (or null (real (0))))
        (make-instance 'redis-cache-backend
                       :stream stream
                       :send-fn send-fn
                       :host host
                       :port port
                       :username (opt :username nil)
                       :password (opt :password nil)
                       :db db
                       :tls (opt :tls nil)
                       :tls-verify tls-verify
                       :client-name client-name
                       :connect-timeout connect-timeout
                       :retries retries
                       :retry-delay retry-delay)))))

(defun use-redis-cache-backend (&rest args &key &allow-other-keys)
  (setf cache-protocol:*cache-backend* (apply #'make-redis-cache-backend args)))

(defmacro with-redis-cache ((var &rest args) &body body)
  "Bind VAR to (MAKE-REDIS-CACHE-BACKEND ,@ARGS) around BODY, then CLOSE it."
  `(let ((,var (make-redis-cache-backend ,@args)))
     (unwind-protect (progn ,@body)
       (close ,var))))

;;; --- transport -----------------------------------------------------------

(defun %endpoint (backend)
  (if (%managed-p backend)
      (format nil "~A:~D" (%host-text (redis-cache-backend-host backend))
              (redis-cache-backend-port backend))
      "redis stream"))

(defun %call-with-io-deadline (backend thunk)
  "Call THUNK. On SBCL a managed connection runs it under a deadline of
CONNECT-TIMEOUT seconds, so a command (or the connect and TLS handshake)
that the server does not answer in time fails instead of blocking for
ever. The socket stream's own timeout (usocket's :TIMEOUT) bounds each wait
on a plain connection, but SBCL does not apply it on Windows, and cl+ssl
reads the socket itself."
  (let ((seconds (and (%managed-p backend)
                      (slot-value backend 'connect-timeout))))
    (if seconds
        #+sbcl (sb-sys:with-deadline (:seconds seconds) (funcall thunk))
        #-sbcl (funcall thunk)
        (funcall thunk))))

(defmacro %with-transport-errors ((backend &key (deadline t)) &body body)
  "Run BODY; a socket, name lookup, stream or TLS failure becomes
CACHE-CONNECTION-ERROR with the original condition as its CAUSE (the
report prints it). The decoder's own CACHE-CONNECTION-ERROR (EOF mid-reply)
has no endpoint; it is re-signalled with the endpoint, the decoder's
condition as its cause. With DEADLINE (the default) BODY runs under
%CALL-WITH-IO-DEADLINE, and a deadline that passes is a transport failure
too."
  `(handler-case ,(if deadline
                      `(%call-with-io-deadline ,backend (lambda () ,@body))
                      `(progn ,@body))
     ;; cl+ssl exports no base condition; CL+SSL-ERROR is its root. On
     ;; SBCL usocket leaves some failures unmapped: getaddrinfo's
     ;; EAI_ADDRFAMILY (::1 on a host without IPv6) as
     ;; SB-BSD-SOCKETS:NAME-SERVICE-ERROR, and EAFNOSUPPORT from connect on
     ;; Windows as an SB-BSD-SOCKETS:SOCKET-ERROR.
     ((or usocket:socket-error usocket:ns-error stream-error cl+ssl::cl+ssl-error
          #+sbcl sb-bsd-sockets:socket-error
          #+sbcl sb-bsd-sockets:name-service-error
          #+sbcl sb-sys:deadline-timeout)
         (e)
       (error 'cache-connection-error :message (%endpoint ,backend) :cause e))
     (cache-connection-error (e)
       (if (cache-protocol:cache-connection-error-cause e)
           (error e)
           (error 'cache-connection-error
                  :message (%endpoint ,backend)
                  :cause (cache-error-message e))))))

(defun %drop-connection (backend)
  "Close the managed connection, if any. A caller's :STREAM is left alone."
  (when (%managed-p backend)
    (let ((stream (redis-cache-backend-stream backend))
          (socket (%socket backend)))
      (setf (redis-cache-backend-stream backend) nil
            (%socket backend) nil)
      ;; Teardown of a socket that may already be dead: only its own
      ;; transport errors are expected here, and nothing can be done about them.
      (handler-case
          (progn
            (when stream (close stream :abort t))
            (when socket (usocket:socket-close socket)))
        ((or stream-error usocket:socket-error cl+ssl::cl+ssl-error) () nil))
      (and socket t))))

(defun %write-command (stream parts)
  (write-sequence (encode-resp3-command parts) stream)
  (force-output stream))

(defun %roundtrip (backend stream parts)
  "Send PARTS on STREAM and read one reply. → raw RESP3 value. When this
does not complete (a transport or protocol error, or a non-local exit such
as a timeout around the call), a managed connection is dropped: its stream
may still hold part of the reply, which the next command would read as its
own."
  (let ((done nil))
    (unwind-protect
        (multiple-value-prog1
            (%with-transport-errors (backend)
              (%write-command stream parts)
              (decode-resp3 stream))
          (setf done t))
      (unless done
        (%drop-connection backend)))))

(defun %hello-unsupported-p (reply)
  "Redis before 6 has no HELLO; a server without RESP3 answers NOPROTO."
  (let ((text (resp-error-message reply)))
    (or (eql 0 (search "NOPROTO" text))
        (and (eql 0 (search "ERR" text))
             (search "unknown command" text :test #'char-equal)))))

(defun %server-error (reply what)
  (error 'cache-error
         :message (format nil "~A: ~A" what (resp-error-message reply))))

(defun %expect-ok (backend stream parts)
  (let ((reply (%roundtrip backend stream parts)))
    (when (resp-error-p reply)
      (%server-error reply (first parts)))
    reply))

(defun %handshake (backend stream)
  "HELLO 3 [AUTH user pass] [SETNAME name], then SELECT. A server without
HELLO gets AUTH / CLIENT SETNAME and stays on RESP2, which DECODE-RESP3
also reads."
  (with-slots (username password db client-name) backend
    (let ((reply (%roundtrip backend stream
                             (append (list "HELLO" "3")
                                     (when password
                                       (list "AUTH" (or username "default") password))
                                     (when client-name
                                       (list "SETNAME" client-name))))))
      (when (resp-error-p reply)
        (unless (%hello-unsupported-p reply)
          (%server-error reply "HELLO"))
        (when password
          (%expect-ok backend stream
                      (append (list "AUTH") (when username (list username))
                              (list password))))
        (when client-name
          (%expect-ok backend stream (list "CLIENT" "SETNAME" client-name)))))
    (unless (eql db 0)
      (%expect-ok backend stream (list "SELECT" (princ-to-string db))))))

(defun %connect (backend)
  "Open the managed connection and run the handshake. → the stream."
  (with-slots (host port tls tls-verify connect-timeout) backend
    (let ((socket (%with-transport-errors (backend :deadline nil)
                    ;; usocket's own :TIMEOUT bounds the connect; a deadline around it
                    ;; would only replace usocket's condition in CAUSE.
                    (usocket:socket-connect host port
                                            :element-type '(unsigned-byte 8)
                                            :timeout connect-timeout
                                            :nodelay t)))
          (done nil))
      (setf (%socket backend) socket)
      (unwind-protect
           (let ((stream (%with-transport-errors (backend)
                           (if tls
                               (cl+ssl:make-ssl-client-stream
                                (usocket:socket-stream socket)
                                :hostname host :verify tls-verify)
                               (usocket:socket-stream socket)))))
             (setf (redis-cache-backend-stream backend) stream)
             (%handshake backend stream)
             (setf done t
                   (%ever-connected backend) t)
             stream)
        (unless done
          (%drop-connection backend))))))

(defun %ensure-stream (backend)
  (or (redis-cache-backend-stream backend)
      (if (%managed-p backend)
          (%connect backend)
          (error 'cache-error
                 :message "redis-cache-backend has no :host, :url, :stream or :send-fn"))))

(defun %retry-pause (backend failed-attempt)
  "Pause before the resend that follows FAILED-ATTEMPT (0-based). The first
resend (an idle socket the server dropped) goes at once; later ones wait
RETRY-DELAY seconds, doubled each time and capped at 2 s, so a server that
is restarting gets time to come back."
  (when (plusp failed-attempt)
    (let ((delay (slot-value backend 'retry-delay)))
      (when (plusp delay)
        (sleep (min 2 (* delay (expt 2 (1- failed-attempt)))))))))

(defun %call (backend parts)
  "Send command PARTS and return the raw reply (a RESP-ERROR is returned,
not signalled). On a managed connection a transport failure drops the
socket. Once the backend has had a connection (%EVER-CONNECTED: in this
call or in any earlier one, including a call that gave up), every
failure, a refused reconnect included, is retried on a new connection, up
to RETRIES resends per call (see %RETRY-PAUSE). A backend that never got
a connection fails at once (wrong host or port). Past that the
CACHE-CONNECTION-ERROR propagates with a RECONNECT restart."
  (let ((fn (redis-cache-backend-send-fn backend)))
    (if fn
        (funcall fn parts)
        (bt2:with-recursive-lock-held ((%lock backend))
          (loop for attempt from 0
                do (restart-case
                       (handler-bind
                           ((cache-connection-error
                             (lambda (e)
                               (declare (ignore e))
                               (%drop-connection backend)
                               (when (and (%ever-connected backend)
                                          (< attempt (redis-cache-backend-retries backend)))
                                 (invoke-restart 'reconnect)))))
                         (return (%roundtrip backend (%ensure-stream backend) parts)))
                     (reconnect ()
                       :report "Reconnect to the server and send the command again."
                       :test (lambda (c)
                               (declare (ignore c))
                               (%managed-p backend))
                       (%retry-pause backend attempt))))))))

(defun %command (backend parts &key (key (second parts)))
  "Like %CALL, but an error reply signals CACHE-ERROR with the server's text
and KEY (by default the command's first argument)."
  (let ((reply (%call backend parts)))
    (when (resp-error-p reply)
      (error 'cache-error :key key
                          :message (resp-error-message reply)))
    reply))

(defmethod close ((backend redis-cache-backend) &key abort)
  "Release the managed connection. → T if one was open. A caller's
:STREAM is not closed. The backend reconnects if it is used again."
  (declare (ignore abort))
  (bt2:with-recursive-lock-held ((%lock backend))
    (%drop-connection backend)))
