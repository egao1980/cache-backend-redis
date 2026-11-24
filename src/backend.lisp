(in-package #:cache-backend-redis)

(defparameter +cas-lua+
  "local v = redis.call('GET', KEYS[1])
if ARGV[1] == '1' then
  if v ~= false then return 0 end
elseif v ~= ARGV[2] then
  return 0
end
if ARGV[4] == '' then
  redis.call('SET', KEYS[1], ARGV[3])
else
  redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
end
return 1"
  "Lua compare-and-set. EVAL with 1 key and ARGV: absent-flag (\"1\": the
key must be missing, i.e. expected NIL), expected, new, and PX
milliseconds or \"\" (no expiry). → 1 when swapped, else 0.")

(defun %as-string (value)
  (cond
    ((eq value :resp-null) nil)
    ((resp-simple-string-p value) (resp-simple-string-value value))
    ((resp-error-p value)
     (error 'cache-error :message (resp-error-message value)))
    ((stringp value) value)
    ((integerp value) (princ-to-string value))
    ((and (vectorp value) (not (stringp value)))
     (%utf8-string value))
    (t value)))

(defmethod cache-get ((cache redis-cache-backend) key &key)
  (%as-string (%command cache (list "GET" key))))

(defun %ttl-milliseconds (ttl)
  "TTL seconds as integer milliseconds, at least 1 for a positive TTL."
  (let ((ms (round (* ttl 1000))))
    (if (plusp ttl) (max 1 ms) ms)))

(defun %expiry-args (ttl)
  "SET arguments for TTL seconds: EX for an integer, PX for a fraction. A
non-positive TTL is sent as is, and the server refuses it."
  (check-type ttl (or null real))
  (cond
   ((null ttl) nil)
   ((integerp ttl) (list "EX" (princ-to-string ttl)))
   (t (list "PX" (princ-to-string (%ttl-milliseconds ttl))))))

(defmethod cache-put ((cache redis-cache-backend) key value &key ttl)
  (%command cache (append (list "SET" key (princ-to-string value))
                          (%expiry-args ttl)))
  value)

(defmethod cache-delete ((cache redis-cache-backend) key &key)
  (let ((n (%command cache (list "DEL" key))))
    (and (integerp n) (plusp n))))

(defmethod cache-incr ((cache redis-cache-backend) key &key (delta 1))
  (check-type delta integer)
  (let ((reply (%command cache (if (= delta 1)
                                   (list "INCR" key)
                                   (list "INCRBY" key (princ-to-string delta))))))
    (unless (integerp reply)
      (error 'cache-error :key key
                          :message (format nil "INCR replied ~s" reply)))
    reply))

(defmethod cache-cas ((cache redis-cache-backend) key expected new &key ttl)
  (check-type ttl (or null real))
  (let ((reply (%command cache (list "EVAL" +cas-lua+ "1" key
                                     (if (null expected) "1" "0")
                                     (if (null expected) "" (princ-to-string expected))
                                     (princ-to-string new)
                                     (if ttl (princ-to-string (%ttl-milliseconds ttl)) "")))))
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
   (clock :initarg :clock :accessor fake-redis-clock :initform #'get-universal-time)
   (password :initarg :password :accessor fake-redis-password :initform nil
             :documentation "When set, HELLO AUTH / AUTH must present it.")))

(defun make-fake-redis (&key clock password)
  (let ((server (make-instance 'fake-redis :password password)))
    (when clock
      (setf (fake-redis-clock server) clock))
    server))

(defun %fr-now (server)
  (funcall (fake-redis-clock server)))

(defun %fr-get (server key)
  (let ((entry (gethash key (fake-redis-table server))))
    (cond
     ((null entry) nil)
     ((and (cdr entry) (<= (cdr entry) (%fr-now server)))
      (remhash key (fake-redis-table server))
      nil)
     (t (car entry)))))

(defun %fr-set (server key value &key ttl)
  (setf (gethash key (fake-redis-table server))
        (cons value (and ttl (+ (%fr-now server) ttl))))
  (make-resp-simple-string "OK"))

(defun %fr-integer (text)
  "TEXT as an integer if Redis string2ll reads it, else NIL: an optional
minus sign, then a non-zero digit and more digits, or a lone \"0\"; the
value must fit in (signed-byte 64). \"007\" and \"-0\" are refused."
  (and (stringp text)
       (let* ((len (length text))
              (start (if (and (plusp len) (char= #\- (char text 0))) 1 0)))
         (and (< start len)
              (or (string= text "0")
                  (char<= #\1 (char text start) #\9))
              (every (lambda (c) (char<= #\0 c #\9)) (subseq text start))
              (let ((n (parse-integer text)))
                (and (typep n '(signed-byte 64)) n))))))

(defun %fr-expiry (args)
  "Seconds from EX n / PX ms in SET ARGS: NIL, a real, or :invalid."
  (let* ((ex (position "EX" args :test #'string-equal))
         (px (position "PX" args :test #'string-equal))
         (n (cond (ex (%fr-integer (nth (1+ ex) args)))
                  (px (%fr-integer (nth (1+ px) args))))))
    (cond
     ((not (or ex px)) nil)
     ((or (null n) (<= n 0)) :invalid)
     (ex n)
     (t (/ n 1000)))))

(defun %fr-incr (server key delta)
  (let* ((cur (%fr-get server key))
         (n (if cur (%fr-integer cur) 0)))
    (cond
     ((null n)
      (make-resp-error "ERR value is not an integer or out of range"))
     ((not (typep (+ n delta) '(signed-byte 64)))
      (make-resp-error "ERR increment or decrement would overflow"))
     (t
      (let ((entry (gethash key (fake-redis-table server))))
        (setf (gethash key (fake-redis-table server))
              (cons (princ-to-string (+ n delta)) (and cur (cdr entry))))
        (+ n delta))))))

(defun %fr-hello (server args)
  "HELLO 3 [AUTH user pass] [SETNAME name] → a RESP3 map, or WRONGPASS / NOAUTH."
  (let* ((auth (position "AUTH" args :test #'string-equal))
         (given (and auth (nth (+ auth 2) args))))
    (cond
     ((null (fake-redis-password server))
      (make-resp-map (list (cons "server" "fake-redis")
                           (cons "version" "7.2.0")
                           (cons "proto" 3))))
     ((null auth)
      (make-resp-error "NOAUTH HELLO must be called with the client already authenticated"))
     ((equal given (fake-redis-password server))
      (%fr-hello (make-fake-redis) nil))
     (t (make-resp-error
         "WRONGPASS invalid username-password pair or user is disabled.")))))

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
             (nx (find "NX" args :test #'string-equal))
             (ttl (%fr-expiry args)))
        (cond
         ((eq ttl :invalid)
          (make-resp-error "ERR invalid expire time in 'set' command"))
         ((and nx (%fr-get server key)) :resp-null)
         (t (%fr-set server key value :ttl ttl)))))
     ((string= cmd "DEL")
      (let ((n 0))
        (dolist (k args)
          (when (%fr-get server k)
            (remhash k (fake-redis-table server))
            (incf n)))
        n))
     ((string= cmd "INCR")
      (%fr-incr server (first args) 1))
     ((string= cmd "INCRBY")
      (let ((delta (%fr-integer (second args))))
        (if delta
            (%fr-incr server (first args) delta)
            (make-resp-error "ERR value is not an integer or out of range"))))
     ((string= cmd "EVAL")
      ;; The +CAS-LUA+ contract: (EVAL script 1 key absent-flag expected new px)
      (destructuring-bind (key absent expected new px)
          (subseq args 2 7)
        (let ((actual (%fr-get server key))
              (ms (%fr-integer px)))
          (cond
           ((not (if (string= absent "1")
                     (null actual)
                     (equal actual expected)))
            0)
           ((string= px "") (%fr-set server key new) 1)
           ((or (null ms) (<= ms 0))
            (make-resp-error
             "ERR Error running script: invalid expire time in 'set' command"))
           (t (%fr-set server key new :ttl (/ ms 1000)) 1)))))
     ((string= cmd "HELLO") (%fr-hello server args))
     ((string= cmd "AUTH")
      (if (equal (car (last args)) (fake-redis-password server))
          (make-resp-simple-string "OK")
          (make-resp-error
           "WRONGPASS invalid username-password pair or user is disabled.")))
     ((member cmd '("SELECT" "WATCH" "MULTI") :test #'string=)
      (make-resp-simple-string "OK"))
     ((string= cmd "PING") (make-resp-simple-string "PONG"))
     ((string= cmd "EXEC") nil)
     (t (make-resp-error (format nil "ERR unknown command '~a'" cmd))))))

(defun fake-redis-send-fn (server)
  "send-fn for REDIS-CACHE-BACKEND: command list → decoded RESP value."
  (lambda (parts)
    (fake-redis-dispatch server parts)))
