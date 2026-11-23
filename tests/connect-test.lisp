(in-package #:cache-backend-redis/tests)

;;; Evidence for egao1980/cache-backend-redis#3: connect by host/port,
;;; AUTH, SELECT, HELLO 3, reconnect, close.
;;;
;;; Spec (redis-py shaped): MAKE-REDIS-CACHE-BACKEND accepts :host :port
;;; :password :db (and :url "redis://[user]:pass@host:port/db"), sends
;;; HELLO 3 / AUTH / SELECT on connect, reconnects after a dropped
;;; socket, and CL:CLOSE on the backend releases the connection.
;;;
;;; Live tests need a password-protected Valkey; without one they skip:
;;;   docker run -d --name valkey-test -p 127.0.0.1:16390:6379 valkey/valkey:8 \
;;;     valkey-server --save "" --appendonly no --requirepass s3cret
;;; Override with CACHE_REDIS_TEST_HOST / _PORT / _PASSWORD.
;;; A side connection (usocket + this library's exported RESP codec)
;;; checks what the server sees.

(defun %env (name default)
  (let ((v (uiop:getenv name)))
    (if (and v (plusp (length v))) v default)))

(defun %live-host () (%env "CACHE_REDIS_TEST_HOST" "127.0.0.1"))
(defun %live-port () (parse-integer (%env "CACHE_REDIS_TEST_PORT" "16390")))
(defun %live-password () (%env "CACHE_REDIS_TEST_PASSWORD" "s3cret"))

(defun %connect (&rest args)
  "MAKE-REDIS-CACHE-BACKEND through APPLY, so an unknown keyword is a
runtime failure of this test rather than a compile-time warning."
  (apply #'make-redis-cache-backend args))

(defun %side-open ()
  "An authenticated side connection to the live server, or NIL."
  (let ((sock (handler-case
                  (usocket:socket-connect (%live-host) (%live-port)
                                          :element-type '(unsigned-byte 8)
                                          :timeout 2)
                (error () nil))))
    (when sock
      (%side sock "AUTH" (%live-password))
      sock)))

(defun %side (sock &rest parts)
  (let ((s (usocket:socket-stream sock)))
    (write-sequence (encode-resp3-command parts) s)
    (force-output s)
    (decode-resp3 s)))

(defmacro with-side ((var) &body body)
  `(let ((,var (%side-open)))
     (if (null ,var)
         (skip (format nil "no Valkey at ~A:~D (see tests/connect-test.lisp)"
                       (%live-host) (%live-port)))
         (unwind-protect (progn ,@body)
           (ignore-errors (usocket:socket-close ,var))))))

(defun %client-list (side)
  "Alist (id . line) from CLIENT LIST, excluding the side connection."
  (let ((me (%side side "CLIENT" "ID"))
        (text (%side side "CLIENT" "LIST")))
    (loop for line in (uiop:split-string (string-right-trim '(#\Newline #\Return) text)
                                         :separator '(#\Newline))
          for id = (let ((p (search "id=" line)))
                     (and p (parse-integer line :start (+ p 3) :junk-allowed t)))
          when (and id (/= id me))
            collect (cons id line))))

(defun %new-clients (side before)
  (remove-if (lambda (e) (assoc (car e) before)) (%client-list side)))

(defun %key (tag)
  (format nil "evidence:~A:~D:~D" tag (get-universal-time) (random 1000000)))

(defun %side-get (side db key)
  (%side side "SELECT" (princ-to-string db))
  (let ((v (%side side "GET" key)))
    (if (eq v :resp-null) nil v)))

(deftest connect-refused-signals-cache-error
  ;; No service needed: nothing listens on 127.0.0.1:1.
  (ok (signals (let ((c (%connect :host "127.0.0.1" :port 1)))
                 (cache-protocol:cache-get c "k"))
               'cache-protocol:cache-error)
      "a refused :host/:port connection signals cache-error"))

(deftest connect-auth-select-db
  (with-side (side)
    (let ((key (%key "db3")))
      (let ((c (%connect :host (%live-host) :port (%live-port)
                         :password (%live-password) :db 3)))
        (unwind-protect
             (progn
               (cache-protocol:cache-put c key "in-db-3")
               (ok (equal "in-db-3" (cache-protocol:cache-get c key))
                   "put/get after AUTH"))
          (close c)))
      (ok (equal "in-db-3" (%side-get side 3 key)) "value landed in db 3")
      (ok (null (%side-get side 0 key)) "and not in db 0"))))

(deftest connect-speaks-resp3-and-close-releases
  (with-side (side)
    (let* ((before (%client-list side))
           (c (%connect :host (%live-host) :port (%live-port)
                        :password (%live-password)))
           (ours nil))
      (unwind-protect
           (progn
             (cache-protocol:cache-put c (%key "resp3") "x")
             (setf ours (%new-clients side before))
             (ok ours "the backend holds a connection")
             (ok (and ours (every (lambda (e) (search " resp=3 " (cdr e))) ours))
                 (format nil "HELLO 3 was sent: ~{~A~^ | ~}" (mapcar #'cdr ours))))
        (close c))
      (sleep 0.2)
      (let ((still (remove-if-not (lambda (e) (assoc (car e) ours))
                                  (%client-list side))))
        (ok (and ours (null still)) "CLOSE ends the server-side connection")))))

(deftest reconnect-after-dropped-socket
  (with-side (side)
    (let* ((key (%key "reconnect"))
           (before (%client-list side))
           (c (%connect :host (%live-host) :port (%live-port)
                        :password (%live-password) :db 5)))
      (unwind-protect
           (progn
             (cache-protocol:cache-put c key "kept")
             (dolist (e (%new-clients side before))
               (%side side "CLIENT" "KILL" "ID" (princ-to-string (car e))))
             (ok (equal "kept" (cache-protocol:cache-get c key))
                 "get after the server dropped the socket reconnects (AUTH + SELECT 5 again)"))
        (close c)))))

(deftest connect-by-url
  (with-side (side)
    (let ((key (%key "url"))
          (c (%connect :url (format nil "redis://:~A@~A:~D/6"
                                    (%live-password) (%live-host) (%live-port)))))
      (unwind-protect (cache-protocol:cache-put c key "via-url")
        (close c))
      (ok (equal "via-url" (%side-get side 6 key))
          "redis://:password@host:port/6 selects db 6"))))
