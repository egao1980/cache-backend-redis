(in-package #:cache-backend-redis/tests)

;;; cache-backend-redis#1: blob lengths are UTF-8 octet counts; replies are
;;; read by octet length and decoded as UTF-8; a value cannot inject frames.

(defun %u (&rest codes) (map 'string #'code-char codes))

(defparameter +key+ (%u #x43a #x43b #x44e #x447 #x20 #x2603)) ; "ключ ☃"
(defparameter +value+ (concatenate 'string (%u #x65e5 #x672c #x8a9e) "-"
                                   (%u #x1F600) "-caf" (%u #xe9)))

(defun %octets (string) (encoding-protocol:encode string :encoding :utf-8))

(defun %decode-all (octets)
  "Every RESP3 value in OCTETS, in order."
  (let ((in (flexi-streams:make-in-memory-input-stream octets)))
    (loop while (listen in) collect (decode-resp3 in))))

(deftest blob-length-counts-octets
  (let* ((wire (encode-resp3-to-octets +value+))
         (n (length (%octets +value+)))
         (header (%octets (format nil "$~d~c~c" n #\Return #\Newline))))
    (ok (/= n (length +value+)) "fixture really is multi-byte")
    (ok (equalp header (subseq wire 0 (length header))))
    (ok (= (length wire) (+ (length header) n 2)))
    (ok (string= +value+ (decode-resp3-from-octets wire)))))

(deftest non-ascii-command-round-trip
  (let ((wire (encode-resp3-command (list "SET" +key+ +value+))))
    (ok (equal (list "SET" +key+ +value+) (decode-resp3-from-octets wire)))
    (ok (equal (list (list "SET" +key+ +value+)) (%decode-all wire))
        "exactly one frame")))

(deftest non-ascii-simple-string-and-error
  (let ((v (decode-resp3-from-octets
            (encode-resp3-to-octets (make-resp-simple-string +value+)))))
    (ok (string= +value+ (resp-simple-string-value v))))
  (let ((v (decode-resp3-from-octets
            (encode-resp3-to-octets (make-resp-error +key+)))))
    (ok (string= +key+ (resp-error-message v)))))

(deftest crlf-value-cannot-inject-command
  (let* ((evil (format nil "x~c~c*1~c~c$8~c~cFLUSHALL~c~c"
                       #\Return #\Newline #\Return #\Newline
                       #\Return #\Newline #\Return #\Newline))
         (evil-utf8 (concatenate 'string +value+ evil))
         (wire (encode-resp3-command (list "SET" "k" evil-utf8)))
         (frames (%decode-all wire)))
    (ok (= 1 (length frames)) "one command on the wire, not two")
    (ok (equal (list "SET" "k" evil-utf8) (first frames)))
    (ok (notany (lambda (f) (equal f '("FLUSHALL"))) frames))))

(deftest crlf-in-simple-string-refused
  (ok (signals (encode-resp3-to-octets
                (make-resp-simple-string (format nil "OK~c~c:1" #\Return #\Newline)))
               'cache-protocol:cache-error))
  (ok (signals (encode-resp3-to-octets
                (make-resp-error (format nil "ERR~c+OK" #\Newline)))
               'cache-protocol:cache-error)))

(deftest blob-length-mismatch-detected
  ;; Declares 3 octets but 4 follow before CRLF: refuse, do not resync.
  (ok (signals (decode-resp3-from-octets
                (%octets (format nil "$3~c~cabcd~c~c" #\Return #\Newline #\Return #\Newline)))
               'cache-protocol:cache-error))
  (ok (signals (decode-resp3-from-octets
                (%octets (format nil "$10~c~cabc" #\Return #\Newline)))
               'cache-protocol:cache-error)
      "truncated blob"))

(deftest invalid-utf8-reply
  (let ((wire (concatenate '(vector (unsigned-byte 8))
                           (%octets (format nil "$2~c~c" #\Return #\Newline))
                           #(#xff #xfe 13 10))))
    (ok (signals (decode-resp3-from-octets wire) 'cache-protocol:cache-error))
    (ok (eq :raw
            (handler-bind ((cache-protocol:cache-error
                             (lambda (c) (declare (ignore c))
                               (invoke-restart 'use-value :raw))))
              (decode-resp3-from-octets wire)))
        "USE-VALUE restart")))

(defun %wire-send-fn (server)
  "A mock RESP3 server: the command goes through the wire codec both ways."
  (lambda (parts)
    (let* ((request (decode-resp3-from-octets (encode-resp3-command parts)))
           (reply (fake-redis-dispatch server request)))
      (decode-resp3-from-octets (encode-resp3-to-octets reply)))))

(deftest backend-non-ascii-round-trip
  (let ((cache (make-redis-cache-backend :send-fn (%wire-send-fn (make-fake-redis)))))
    (cache-protocol:cache-put cache +key+ +value+)
    (ok (string= +value+ (cache-protocol:cache-get cache +key+)))
    (ok (null (cache-protocol:cache-get cache "missing")))
    (ok (cache-protocol:cache-delete cache +key+))
    (ok (null (cache-protocol:cache-get cache +key+)))))
