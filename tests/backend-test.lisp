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

(deftest stream-roundtrip-via-string-streams
  (let* ((fake (make-fake-redis))
         (c (make-redis-cache-backend
             :send-fn
             (lambda (parts)
               (let ((wire (encode-resp3-command parts)))
                 (let ((decoded (decode-resp3-from-string wire)))
                   (fake-redis-dispatch fake decoded)))))))
    (cache-protocol:cache-put c "s" "z")
    (ok (string= "z" (cache-protocol:cache-get c "s")))))
