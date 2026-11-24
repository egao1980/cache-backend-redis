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
               (redis-cache-backend-host backend)
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

(defun %parse-db (text url)
  (handler-case (let ((n (parse-integer text)))
                  (check-type n (integer 0))
                  n)
    ((or parse-error type-error) ()
      (error 'cache-error
             :message (format nil "bad database number ~s in ~a" text url)))))

(defun parse-redis-url (url)
  "Parse redis://[[user]:password@]host[:port][/db] (rediss:// for TLS).
→ plist :host :port :username :password :db :tls."
  (let* ((uri (quri:uri url))
         (scheme (and (quri:uri-scheme uri)
                      (string-downcase (quri:uri-scheme uri)))))
    (unless (member scheme '("redis" "rediss") :test #'equal)
      (error 'cache-error
             :message (format nil "not a redis:// or rediss:// URL: ~a" url)))
    (unless (and (quri:uri-host uri) (plusp (length (quri:uri-host uri))))
      (error 'cache-error :message (format nil "no host in ~a" url)))
    (let* ((userinfo (quri:uri-userinfo uri))
           (colon (and userinfo (position #\: userinfo)))
           (user (and userinfo
                      (quri:url-decode (subseq userinfo 0 (or colon (length userinfo))))))
           (password (and colon (quri:url-decode (subseq userinfo (1+ colon)))))
           (path (string-trim "/" (or (quri:uri-path uri) ""))))
      (list :host (quri:uri-host uri)
            :port (or (quri:uri-port uri) 6379)
            :username (and user (plusp (length user)) user)
            :password (and password (plusp (length password)) password)
            :db (if (plusp (length path)) (%parse-db path url) 0)
            :tls (string= scheme "rediss")))))

(defun make-redis-cache-backend (&rest args
                                       &key stream send-fn url host port username
                                       password db tls (tls-verify :required)
                                       client-name (connect-timeout 5) (retries 1)
                                       (retry-delay 1/10))
  "A RESP3 backend. Either:
 - :HOST [:PORT 6379] [:USERNAME] [:PASSWORD] [:DB 0] [:TLS] or :URL
   \"redis://user:pass@host:port/db\" (\"rediss://\" for TLS): the backend
   owns a connection. It connects on first use, sends HELLO 3 (with AUTH
   and SETNAME), SELECT DB. Once it has had a connection, a dropped socket
   (or a refused reconnect) is retried on a new connection, up to RETRIES
   resends: the first at once, then after RETRY-DELAY seconds, doubled each
   time, capped at 2 s. Explicit keywords win over URL parts. TLS-VERIFY is
   cl+ssl's :VERIFY (:REQUIRED by default). CL:CLOSE or WITH-REDIS-CACHE
   releases the connection.
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
      (format nil "~A:~D" (redis-cache-backend-host backend)
              (redis-cache-backend-port backend))
      "redis stream"))

(defmacro %with-transport-errors ((backend) &body body)
  "Run BODY; a socket, stream or TLS failure becomes CACHE-CONNECTION-ERROR
with the original condition as its CAUSE (the report prints it). The
decoder's own CACHE-CONNECTION-ERROR (EOF mid-reply) has no endpoint; it
is re-signalled with the endpoint, the decoder's condition as its cause."
  `(handler-case (progn ,@body)
     ;; cl+ssl exports no base condition; CL+SSL-ERROR is its root.
     ((or usocket:socket-error usocket:ns-error stream-error cl+ssl::cl+ssl-error) (e)
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
  "Send PARTS on STREAM and read one reply. → raw RESP3 value."
  (%with-transport-errors (backend)
    (%write-command stream parts)
    (decode-resp3 stream)))

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
    (let ((socket (%with-transport-errors (backend)
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
             (setf done t)
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
socket. Once a connection has been up (at entry, or opened by an earlier
attempt of this call), every failure, a refused reconnect included, is
retried on a new connection, up to RETRIES resends (see %RETRY-PAUSE).
A backend that never got a connection fails at once. Past that the
CACHE-CONNECTION-ERROR propagates with a RECONNECT restart."
  (let ((fn (redis-cache-backend-send-fn backend)))
    (if fn
        (funcall fn parts)
        (bt2:with-recursive-lock-held ((%lock backend))
          (loop with established = (redis-cache-backend-connected-p backend)
                for attempt from 0
                do (restart-case
                       (handler-bind
                           ((cache-connection-error
                             (lambda (e)
                               (declare (ignore e))
                               (%drop-connection backend)
                               (when (and established
                                          (< attempt (redis-cache-backend-retries backend)))
                                 (invoke-restart 'reconnect)))))
                         (let ((stream (%ensure-stream backend)))
                           (when (%managed-p backend)
                             (setf established t))
                           (return (%roundtrip backend stream parts))))
                     (reconnect ()
                       :report "Reconnect to the server and send the command again."
                       :test (lambda (c)
                               (declare (ignore c))
                               (%managed-p backend))
                       (%retry-pause backend attempt))))))))

(defun %command (backend parts)
  "Like %CALL, but an error reply signals CACHE-ERROR with the server's text."
  (let ((reply (%call backend parts)))
    (when (resp-error-p reply)
      (error 'cache-error :key (second parts)
                          :message (resp-error-message reply)))
    reply))

(defmethod close ((backend redis-cache-backend) &key abort)
  "Release the managed connection. → T if one was open. A caller's
:STREAM is not closed. The backend reconnects if it is used again."
  (declare (ignore abort))
  (bt2:with-recursive-lock-held ((%lock backend))
    (%drop-connection backend)))
