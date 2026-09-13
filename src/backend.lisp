(in-package #:cache-backend-redis)

(defparameter +cas-lua+
  "if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('SET', KEYS[1], ARGV[2]) return 1 else return 0 end"
  "Lua stub: compare-and-set. EVAL this with 1 key + expected + new.")

(defclass redis-cache-backend (cache-backend)
  ((stream :initarg :stream :accessor redis-cache-backend-stream :initform nil)
   (send-fn :initarg :send-fn :accessor redis-cache-backend-send-fn :initform nil)))

(defun make-redis-cache-backend (&key stream send-fn)
  (make-instance 'redis-cache-backend :stream stream :send-fn send-fn))

(defun use-redis-cache-backend (&rest args &key &allow-other-keys)
  (setf cache-protocol:*cache-backend* (apply #'make-redis-cache-backend args)))

(defun %as-string (value)
  (cond
    ((eq value :resp-null) nil)
    ((resp-simple-string-p value) (resp-simple-string-value value))
    ((resp-error-p value)
     (error 'cache-error :message (resp-error-message value)))
    ((stringp value) value)
    ((integerp value) (princ-to-string value))
    ((and (vectorp value) (not (stringp value)))
     (map 'string #'code-char value))
    (t value)))

(defun %command (backend parts)
  (let ((fn (redis-cache-backend-send-fn backend))
        (stream (redis-cache-backend-stream backend)))
    (cond
      (fn (funcall fn parts))
      (stream
       (write-string (encode-resp3-command parts) stream)
       (force-output stream)
       (decode-resp3 stream))
      (t (error 'cache-error :message "redis-cache-backend has no stream or send-fn")))))

(defmethod cache-get ((cache redis-cache-backend) key &key)
  (let ((v (%command cache (list "GET" key))))
    (%as-string v)))

(defmethod cache-put ((cache redis-cache-backend) key value &key ttl)
  (let ((reply (if ttl
                   (%command cache (list "SET" key (princ-to-string value)
                                         "EX" (princ-to-string ttl)))
                   (%command cache (list "SET" key (princ-to-string value))))))
    (declare (ignore reply))
    value))

(defmethod cache-delete ((cache redis-cache-backend) key &key)
  (let ((n (%command cache (list "DEL" key))))
    (and (integerp n) (plusp n))))

(defmethod cache-incr ((cache redis-cache-backend) key &key (delta 1))
  (if (= delta 1)
      (%command cache (list "INCR" key))
      (%command cache (list "INCRBY" key (princ-to-string delta)))))

(defmethod cache-cas ((cache redis-cache-backend) key expected new &key ttl)
  (declare (ignore ttl))
  (let ((reply (%command cache (list "EVAL" +cas-lua+ "1" key
                                     (if (null expected) "" (princ-to-string expected))
                                     (princ-to-string new)))))
    (cond
      ((and (integerp reply) (plusp reply)) new)
      (t
       (let ((actual (cache-get cache key)))
         (restart-case
             (error 'cache-cas-mismatch
                    :key key :expected expected :actual actual)
           (use-value (value)
             :report "Use a supplied value (treat CAS as success)"
             value)))))))

;;; ---------------------------------------------------------------------------
;;; In-memory fake RESP server (tests; no live Valkey)
;;; ---------------------------------------------------------------------------

(defclass fake-redis ()
  ((table :initform (make-hash-table :test #'equal) :accessor fake-redis-table)
   (clock :initarg :clock :accessor fake-redis-clock :initform #'get-universal-time)))

(defun make-fake-redis (&key clock)
  (if clock
      (make-instance 'fake-redis :clock clock)
      (make-instance 'fake-redis)))

(defun %fr-now (server)
  (funcall (fake-redis-clock server)))

(defun %fr-get (server key)
  (let ((entry (gethash key (fake-redis-table server))))
    (cond
      ((null entry) nil)
      ((and (cdr entry) (< (cdr entry) (%fr-now server)))
       (remhash key (fake-redis-table server))
       nil)
      (t (car entry)))))

(defun %fr-set (server key value &key ttl)
  (setf (gethash key (fake-redis-table server))
        (cons value (and ttl (+ (%fr-now server) ttl))))
  (make-resp-simple-string "OK"))

(defun fake-redis-dispatch (server parts)
  "PARTS is a list of command strings. → RESP3 Lisp value."
  (let* ((cmd (string-upcase (string (first parts))))
         (args (rest parts)))
    (cond
      ((string= cmd "GET")
       (or (%fr-get server (first args)) :resp-null))
      ((string= cmd "SET")
       (let* ((key (first args))
              (value (second args))
              (ex-pos (position "EX" args :test #'string-equal))
              (nx (find "NX" args :test #'string-equal))
              (ttl (and ex-pos (parse-integer (nth (1+ ex-pos) args)))))
         (if (and nx (%fr-get server key))
             :resp-null
             (%fr-set server key value :ttl ttl))))
      ((string= cmd "DEL")
       (let ((n 0))
         (dolist (k args)
           (when (nth-value 1 (gethash k (fake-redis-table server)))
             (remhash k (fake-redis-table server))
             (incf n)))
         n))
      ((string= cmd "INCR")
       (let* ((cur (%fr-get server (first args)))
              (n (if cur (parse-integer (princ-to-string cur)) 0))
              (next (1+ n)))
         (%fr-set server (first args) (princ-to-string next))
         next))
      ((string= cmd "INCRBY")
       (let* ((cur (%fr-get server (first args)))
              (n (if cur (parse-integer (princ-to-string cur)) 0))
              (delta (parse-integer (second args)))
              (next (+ n delta)))
         (%fr-set server (first args) (princ-to-string next))
         next))
      ((string= cmd "EVAL")
       ;; (EVAL script numkeys key [keys…] arg…)
       (let* ((nkeys (parse-integer (second args)))
              (keys (subseq args 2 (+ 2 nkeys)))
              (evals (subseq args (+ 2 nkeys)))
              (key (first keys))
              (expected (first evals))
              (new (second evals))
              (actual (%fr-get server key)))
         (if (equal (or actual "") expected)
             (progn (%fr-set server key new) 1)
             0)))
      ((string= cmd "WATCH")
       (make-resp-simple-string "OK"))
      ((string= cmd "MULTI")
       (make-resp-simple-string "OK"))
      ((string= cmd "EXEC")
       nil)
      (t (make-resp-error (format nil "ERR unknown command '~a'" cmd))))))

(defun fake-redis-send-fn (server)
  "send-fn for REDIS-CACHE-BACKEND: command list → decoded RESP value."
  (lambda (parts)
    (fake-redis-dispatch server parts)))
