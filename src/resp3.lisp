(in-package #:cache-backend-redis)

;;; RESP3 codec over an (unsigned-byte 8) stream. Text is UTF-8 (encoding-protocol);
;;; blob lengths are octet counts, as the protocol requires.
;;; Types: simple string, error, integer, blob, array, null, map, set, bool, double.

(defconstant +resp-null+ :resp-null)
(defun resp-null () :resp-null)

(defconstant +resp-true+ :resp-true)
(defun resp-true () :resp-true)

(defconstant +resp-false+ :resp-false)
(defun resp-false () :resp-false)

(defclass resp-error ()
  ((message :initarg :message :reader resp-error-message :initform "")))

(defun resp-error-p (x) (typep x 'resp-error))

(defun make-resp-error (message)
  (make-instance 'resp-error :message message))

(defclass resp-simple-string ()
  ((value :initarg :value :reader resp-simple-string-value)))

(defun resp-simple-string-p (x) (typep x 'resp-simple-string))

(defun make-resp-simple-string (value)
  (make-instance 'resp-simple-string :value (string value)))

(defclass resp-map ()
  ((pairs :initarg :pairs :reader resp-map-pairs :initform nil)))

(defun resp-map-p (x) (typep x 'resp-map))

(defun make-resp-map (pairs)
  (make-instance 'resp-map :pairs pairs))

(defclass resp-set ()
  ((members :initarg :members :reader resp-set-members :initform nil)))

(defun resp-set-p (x) (typep x 'resp-set))

(defun make-resp-set (members)
  (make-instance 'resp-set :members members))

(defconstant +cr+ 13)
(defconstant +lf+ 10)

(defparameter *max-bulk-length* (* 512 1024 1024)
  "Largest blob length accepted when decoding (Redis proto-max-bulk-len default).")

(defparameter *max-line-length* (* 64 1024)
  "Largest header / simple-string line accepted when decoding.")

(defparameter *max-nesting-depth* 64
  "Deepest level a value may sit at in a reply when decoding: a top-level
value is at level 1, an element of a top-level array (map, set, push
frame, attribute) at level 2. Redis replies nest a few levels; a deeper
one is refused with CACHE-ERROR instead of exhausting the stack.")

(defvar *decode-depth* 0
  "Nesting depth of the value DECODE-RESP3 is reading (1 for a top-level one).")

;;; --- octets ----------------------------------------------------------------

(defun %utf8-octets (string)
  "STRING → UTF-8 octets through encoding-protocol."
  (encoding-protocol:encode string :encoding :utf-8))

(defun %utf8-string (octets)
  "UTF-8 OCTETS → string. Invalid UTF-8 signals CACHE-ERROR with a USE-VALUE
restart (e.g. to keep the raw octets)."
  (handler-case (encoding-protocol:decode octets :encoding :utf-8)
    (encoding-protocol:encoding-error (e)
      (restart-case
          (error 'cache-error
                 :message (format nil "RESP3 reply is not valid UTF-8: ~a" e))
        (use-value (value)
          :report "Return a supplied value (e.g. the raw octets) instead."
          value)))))

(defun %write-ascii (string stream)
  (loop for c across string do (write-byte (char-code c) stream)))

(defun %write-crlf (stream)
  (write-byte +cr+ stream)
  (write-byte +lf+ stream))

(defun %write-header (type-char n stream)
  (write-byte (char-code type-char) stream)
  (%write-ascii (princ-to-string n) stream)
  (%write-crlf stream))

(defun %write-line-value (type-char string stream)
  "Simple string / error: a single line, so CR or LF in it would end the
frame early and let the rest be read as another reply. Refuse them."
  (when (find-if (lambda (c) (member c '(#\Return #\Newline))) string)
    (error 'cache-error
           :message (format nil "RESP3 ~a line contains CR/LF: ~s" type-char string)))
  (write-byte (char-code type-char) stream)
  (write-sequence (%utf8-octets string) stream)
  (%write-crlf stream))

(defun %write-blob (octets stream)
  (%write-header #\$ (length octets) stream)
  (write-sequence octets stream)
  (%write-crlf stream))

(defun %part-octets (x)
  "A command part as octets: strings UTF-8, octet vectors as is."
  (etypecase x
    (string (%utf8-octets x))
    ((vector (unsigned-byte 8)) (coerce x '(simple-array (unsigned-byte 8) (*))))
    (symbol (%utf8-octets (string x)))
    (integer (%utf8-octets (princ-to-string x)))))

;;; --- encode ----------------------------------------------------------------

(defun encode-resp3 (value stream)
  "Write VALUE as RESP3 to the (unsigned-byte 8) STREAM. Strings are bulk
strings: UTF-8 octets prefixed with their octet count. Wrap with
MAKE-RESP-SIMPLE-STRING for `+` simple strings."
  (cond
    ((eq value :resp-null)
     (%write-ascii "_" stream) (%write-crlf stream))
    ((eq value :resp-true)
     (%write-ascii "#t" stream) (%write-crlf stream))
    ((eq value :resp-false)
     (%write-ascii "#f" stream) (%write-crlf stream))
    ((resp-error-p value)
     (%write-line-value #\- (resp-error-message value) stream))
    ((resp-simple-string-p value)
     (%write-line-value #\+ (resp-simple-string-value value) stream))
    ((integerp value)
     (%write-ascii (format nil ":~d" value) stream)
     (%write-crlf stream))
    ((floatp value)
     (%write-ascii (let ((*read-default-float-format* 'double-float))
                     (format nil ",~a" (float value 1d0)))
                   stream)
     (%write-crlf stream))
    ((resp-map-p value)
     (let ((pairs (resp-map-pairs value)))
       (%write-header #\% (length pairs) stream)
       (dolist (pair pairs)
         (encode-resp3 (car pair) stream)
         (encode-resp3 (cdr pair) stream))))
    ((resp-set-p value)
     (let ((mem (resp-set-members value)))
       (%write-header #\~ (length mem) stream)
       (dolist (m mem) (encode-resp3 m stream))))
    ((typep value '(vector (unsigned-byte 8)))
     (%write-blob value stream))
    ((stringp value)
     (%write-blob (%utf8-octets value) stream))
    ((listp value)
     (%write-header #\* (length value) stream)
     (dolist (elt value) (encode-resp3 elt stream)))
    (t (error 'cache-error
              :message (format nil "cannot encode ~s as RESP3" value))))
  value)

(defun encode-resp3-to-octets (value)
  "VALUE as RESP3 wire octets."
  (coerce (flexi-streams:with-output-to-sequence (sink)
            (encode-resp3 value sink))
          '(simple-array (unsigned-byte 8) (*))))

(defun encode-resp3-to-string (value)
  "VALUE's RESP3 wire octets decoded as UTF-8 (for display and tests; blob
lengths are octet counts, not character counts)."
  (encoding-protocol:decode (encode-resp3-to-octets value) :encoding :utf-8))

(defun encode-resp3-command (parts)
  "Encode a Redis command (list of strings / octet vectors / integers) as the
wire octets of a RESP3 array of bulk strings."
  (encode-resp3-to-octets (mapcar #'%part-octets parts)))

;;; --- decode ----------------------------------------------------------------

(defun %eof ()
  ;; The server closed the connection (or a reply was cut short): a
  ;; transport failure, which a managed connection recovers from by
  ;; reconnecting.
  (error 'cache-connection-error :message "unexpected EOF decoding RESP3"))

(defun %read-line-octets (stream)
  "Octets up to CRLF (CRLF consumed, not returned). A bare LF is accepted
as the terminator; a CR not followed by LF is a protocol error."
  (let ((out (make-array 16 :element-type '(unsigned-byte 8)
                            :adjustable t :fill-pointer 0)))
    (loop
      (let ((b (read-byte stream nil nil)))
        (cond
          ((null b) (%eof))
          ((= b +cr+)
           (let ((n (read-byte stream nil nil)))
             (cond ((null n) (%eof))
                   ((= n +lf+) (return out))
                   (t (error 'cache-error :message "RESP3 CR not followed by LF")))))
          ((= b +lf+) (return out))
          ((>= (length out) *max-line-length*)
           (error 'cache-error :message "RESP3 line too long"))
          (t (vector-push-extend b out)))))))

(defun %parse-int (s)
  (parse-integer s :junk-allowed t))

(defun %ascii (octets &key (start 0))
  (map 'string #'code-char (subseq octets start)))

(defun %read-blob (stream n)
  (when (> n *max-bulk-length*)
    (error 'cache-error :message (format nil "RESP3 blob length ~d too large" n)))
  (let ((buf (make-array n :element-type '(unsigned-byte 8))))
    (unless (= n (read-sequence buf stream))
      (%eof))
    (let ((cr (read-byte stream nil nil))
          (lf (read-byte stream nil nil)))
      (unless (and (eql cr +cr+) (eql lf +lf+))
        (error 'cache-error
               :message "RESP3 blob not followed by CRLF (length mismatch)")))
    buf))

(defun %decimal-number-p (text)
  "TEXT is [sign] digits [. digits] [e [sign] digits], with an ASCII digit
before or after the point."
  (let ((i 0)
        (n (length text)))
    (flet ((sign ()
             (when (and (< i n) (find (char text i) "+-"))
               (incf i)))
           (digits ()
             (let ((start i))
               (loop while (and (< i n) (char<= #\0 (char text i) #\9))
                     do (incf i))
               (- i start))))
      (sign)
      (let* ((int (digits))
             (frac (if (and (< i n) (char= #\. (char text i)))
                       (progn (incf i) (digits))
                       0)))
        (and (plusp (+ int frac))
             (or (= i n)
                 (and (char-equal #\e (char text i))
                      (progn (incf i) (sign) (plusp (digits)))
                      (= i n))))))))

(defun %parse-double (text)
  "RESP3 double TEXT → double-float: a decimal number with an optional
exponent, or inf / -inf. The reader runs only on a decimal number, with
standard syntax, so a reply can intern nothing and run no reader macro.
Anything else (nan included: it has no portable Lisp value) signals
CACHE-ERROR."
  (flet ((bad ()
           (error 'cache-error :message (format nil "bad RESP3 double ~s" text))))
    (cond
     ((string-equal text "inf")
      #+sbcl sb-ext:double-float-positive-infinity
      #-sbcl (bad))
     ((string-equal text "-inf")
      #+sbcl sb-ext:double-float-negative-infinity
      #-sbcl (bad))
     ((%decimal-number-p text)
      (handler-case (with-standard-io-syntax
                        (let ((*read-default-float-format* 'double-float)
                              (*read-eval* nil))
                          (float (read-from-string text) 1d0)))
        ((or reader-error arithmetic-error) () (bad))))
     (t (bad)))))

(defun decode-resp3 (stream)
  "Read one RESP3 value from the (unsigned-byte 8) STREAM. Blob strings are
read by their octet length, then decoded as UTF-8. Verbatim strings lose
their 3-letter format prefix, blob errors become RESP-ERROR, big numbers
are integers, and an attribute map before a reply is skipped. Aggregates
nested deeper than *MAX-NESTING-DEPTH* signal CACHE-ERROR."
  (let ((*decode-depth* (1+ *decode-depth*)))
    (when (> *decode-depth* *max-nesting-depth*)
      (error 'cache-error
             :message (format nil "RESP3 reply nested deeper than ~D levels"
                              *max-nesting-depth*)))
    (let* ((line (%read-line-octets stream))
           (type (if (plusp (length line)) (code-char (aref line 0))
                     (error 'cache-error :message "empty RESP3 line"))))
      (flet ((rest-text () (%utf8-string (subseq line 1)))
             (rest-ascii () (%ascii line :start 1))
             (blob ()
               (let ((n (%parse-int (%ascii line :start 1))))
                 (if (or (null n) (< n 0))
                     nil
                     (%utf8-string (%read-blob stream n))))))
        (case type
          (#\+ (make-resp-simple-string (rest-text)))
          (#\- (make-resp-error (rest-text)))
          (#\: (%parse-int (rest-ascii)))
          (#\( (or (%parse-int (rest-ascii))
                   (error 'cache-error :message (format nil "bad RESP3 big number ~s"
                                                        (rest-ascii)))))
          (#\$ (or (blob) :resp-null))
          (#\! (make-resp-error (or (blob) "")))
          (#\=
           (let ((text (or (blob) "")))
             ;; "txt:..." / "mkd:...": the first four characters are the format.
             (if (and (>= (length text) 4) (char= #\: (char text 3)))
                 (subseq text 4)
                 text)))
          ((#\* #\>)
           (let ((n (%parse-int (rest-ascii))))
             (if (or (null n) (< n 0))
                 :resp-null
                 (loop repeat n collect (decode-resp3 stream)))))
          (#\_ :resp-null)
          (#\#
           (let ((rest (rest-ascii)))
             (cond
              ((string-equal rest "t") :resp-true)
              ((string-equal rest "f") :resp-false)
              (t (error 'cache-error :message (format nil "bad RESP3 bool ~s" rest))))))
          (#\, (%parse-double (rest-ascii)))
          (#\%
           (let ((n (or (%parse-int (rest-ascii)) 0)))
             (make-resp-map
              (loop repeat n
                    collect (let* ((k (decode-resp3 stream))
                                   (v (decode-resp3 stream)))
                              (cons k v))))))
          (#\|
           ;; Attribute map: auxiliary data for the reply that follows.
           (let ((n (or (%parse-int (rest-ascii)) 0)))
             (loop repeat (* 2 n) do (decode-resp3 stream))
             (decode-resp3 stream)))
          (#\~
           (let ((n (or (%parse-int (rest-ascii)) 0)))
             (make-resp-set (loop repeat n collect (decode-resp3 stream)))))
          (t (error 'cache-error
                    :message (format nil "unknown RESP3 type byte ~s" type))))))))

(defun decode-resp3-from-octets (octets)
  "Decode one RESP3 value from wire OCTETS."
  (decode-resp3 (flexi-streams:make-in-memory-input-stream
                 (coerce octets '(vector (unsigned-byte 8))))))

(defun decode-resp3-from-string (string)
  "Decode one RESP3 value from STRING, taken as the UTF-8 text of the wire."
  (decode-resp3-from-octets (%utf8-octets string)))
