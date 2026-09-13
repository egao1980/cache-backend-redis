(in-package #:cache-backend-redis/tests)

(defun %round (value)
  (decode-resp3-from-string (encode-resp3-to-string value)))

(deftest resp3-simple-string
  (let ((v (%round (make-resp-simple-string "OK"))))
    (ok (resp-simple-string-p v))
    (ok (string= "OK" (resp-simple-string-value v)))))

(deftest resp3-error
  (let ((v (%round (make-resp-error "ERR nope"))))
    (ok (resp-error-p v))
    (ok (string= "ERR nope" (resp-error-message v)))))

(deftest resp3-integer
  (ok (= 42 (%round 42)))
  (ok (= -3 (%round -3))))

(deftest resp3-blob
  (ok (string= "foobar" (%round "foobar"))))

(deftest resp3-array
  (ok (equal '("a" "b") (%round '("a" "b")))))

(deftest resp3-null
  (ok (eq :resp-null (%round :resp-null))))

(deftest resp3-bool
  (ok (eq :resp-true (%round :resp-true)))
  (ok (eq :resp-false (%round :resp-false))))

(deftest resp3-double
  (let ((v (%round 1.5d0)))
    (ok (floatp v))
    (ok (< (abs (- v 1.5d0)) 1d-9))))

(deftest resp3-map
  (let ((v (%round (make-resp-map (list (cons "first" 1) (cons "second" 2))))))
    (ok (resp-map-p v))
    (ok (= 2 (length (resp-map-pairs v))))
    (ok (equal "first" (car (first (resp-map-pairs v)))))
    (ok (= 1 (cdr (first (resp-map-pairs v)))))))

(deftest resp3-set
  (let ((v (%round (make-resp-set '("a" "b")))))
    (ok (resp-set-p v))
    (ok (equal '("a" "b") (resp-set-members v)))))

(deftest resp3-command
  (let ((wire (encode-resp3-command '("GET" "k"))))
    (ok (char= #\* (char wire 0)))
    (let ((v (decode-resp3-from-string wire)))
      (ok (equal '("GET" "k") v)))))
