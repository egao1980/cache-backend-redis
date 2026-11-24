(in-package #:cache-backend-redis/tests)

(defun %cache ()
  (let ((fake (make-fake-redis)))
    (values (make-redis-cache-backend :send-fn (fake-redis-send-fn fake))
            fake)))

(deftest redis-get-put-delete
  (multiple-value-bind (c fake) (%cache)
    (declare (ignore fake))
    (cache-protocol:cache-put c "k" "v")
    (ok (string= "v" (cache-protocol:cache-get c "k")))
    (ok (cache-protocol:cache-delete c "k"))
    (ok (null (cache-protocol:cache-get c "k")))))

(deftest redis-incr
  (multiple-value-bind (c fake) (%cache)
    (declare (ignore fake))
    (ok (= 1 (cache-protocol:cache-incr c "n")))
    (ok (= 4 (cache-protocol:cache-incr c "n" :delta 3)))))

(defparameter *incr-cases*
  ;; (stored delta expected): Valkey 8 INCR / INCRBY (cache-protocol#4).
  '(("0" 1 1) ("-1" 1 0) ("10" -20 -10)
    ("9223372036854775806" 1 9223372036854775807)
    ("-9223372036854775807" -1 -9223372036854775808)
    ("007" 1 :error) ("-0" 1 :error) ("+1" 1 :error)
    ("9223372036854775808" 0 :error)
    ("9223372036854775807" 1 :error)
    ("-9223372036854775808" -1 :error)
    ("1" 9223372036854775808 :error)))

(defun %incr-outcome (cache key stored delta)
  (cache-protocol:cache-put cache key stored)
  (handler-case (cache-protocol:cache-incr cache key :delta delta)
    (cache-protocol:cache-error () :error)))

(deftest fake-server-and-in-memory-incr-agree-with-redis
  ;; The fake server's INCR follows string2ll and int64, like Redis and
  ;; like the in-memory cache (cache-protocol#4).
  (loop for (stored delta expected) in *incr-cases*
        do (let ((fake (%incr-outcome (%cache) "n" stored delta))
                 (memory (%incr-outcome (cache-protocol:make-in-memory-ttl-cache)
                                        "n" stored delta)))
             (ok (eql expected fake)
                 (format nil "fake: INCRBY ~S ~S → ~S (got ~S)" stored delta expected fake))
             (ok (eql expected memory)
                 (format nil "in-memory: ~S + ~S → ~S (got ~S)" stored delta expected memory)))))

(deftest redis-cas
  (multiple-value-bind (c fake) (%cache)
    (declare (ignore fake))
    (cache-protocol:cache-put c "k" "old")
    (ok (string= "new" (cache-protocol:cache-cas c "k" "old" "new")))
    (ok (signals (cache-protocol:cache-cas c "k" "old" "x")
                 'cache-protocol:cache-cas-mismatch))))

(deftest redis-cas-use-value
  (multiple-value-bind (c fake) (%cache)
    (declare (ignore fake))
    (cache-protocol:cache-put c "k" "old")
    (let ((got nil))
      (handler-bind ((cache-protocol:cache-cas-mismatch
                      (lambda (cond)
                        (use-value :ok cond))))
        (setf got (cache-protocol:cache-cas c "k" "nope" "x")))
      (ok (eq :ok got)))))

(deftest redis-ttl
  (let* ((now 50)
         (fake (make-fake-redis :clock (lambda () now)))
         (c (make-redis-cache-backend :send-fn (fake-redis-send-fn fake))))
    (cache-protocol:cache-put c "k" "v" :ttl 5)
    (ok (string= "v" (cache-protocol:cache-get c "k")))
    (incf now 6)
    (ok (null (cache-protocol:cache-get c "k")))))

(deftest stream-roundtrip-via-wire-octets
  (let* ((fake (make-fake-redis))
         (c (make-redis-cache-backend
             :send-fn
             (lambda (parts)
               (let ((wire (encode-resp3-command parts)))
                 (let ((decoded (decode-resp3-from-octets wire)))
                   (fake-redis-dispatch fake decoded)))))))
    (cache-protocol:cache-put c "s" "z")
    (ok (string= "z" (cache-protocol:cache-get c "s")))))
