(in-package #:cache-backend-redis)

;;; RESP3 codec over a character or binary stream.
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

(defun %write-crlf (stream)
  (write-char #\Return stream)
  (write-char #\Newline stream))

(defun %utf8 (x)
  (etypecase x
    (string x)
    ((vector (unsigned-byte 8)) (map 'string #'code-char x))
    (symbol (string x))
    (integer (princ-to-string x))))

(defun encode-resp3 (value &optional (stream *standard-output*))
  "Write VALUE as RESP3. Strings default to bulk strings; wrap with
   MAKE-RESP-SIMPLE-STRING for `+` simple strings."
  (cond
    ((eq value :resp-null)
     (write-string "_" stream) (%write-crlf stream))
    ((eq value :resp-true)
     (write-string "#t" stream) (%write-crlf stream))
    ((eq value :resp-false)
     (write-string "#f" stream) (%write-crlf stream))
    ((resp-error-p value)
     (write-char #\- stream)
     (write-string (resp-error-message value) stream)
     (%write-crlf stream))
    ((resp-simple-string-p value)
     (write-char #\+ stream)
     (write-string (resp-simple-string-value value) stream)
     (%write-crlf stream))
    ((integerp value)
     (write-char #\: stream)
     (princ value stream)
     (%write-crlf stream))
    ((floatp value)
     (write-char #\, stream)
     (let ((*read-default-float-format* 'double-float))
       (princ value stream))
     (%write-crlf stream))
    ((resp-map-p value)
     (let ((pairs (resp-map-pairs value)))
       (write-char #\% stream)
       (princ (length pairs) stream)
       (%write-crlf stream)
       (dolist (pair pairs)
         (encode-resp3 (car pair) stream)
         (encode-resp3 (cdr pair) stream))))
    ((resp-set-p value)
     (let ((mem (resp-set-members value)))
       (write-char #\~ stream)
       (princ (length mem) stream)
       (%write-crlf stream)
       (dolist (m mem) (encode-resp3 m stream))))
    ((and (vectorp value) (not (stringp value)))
     (write-char #\$ stream)
     (princ (length value) stream)
     (%write-crlf stream)
     (loop for b across value do (write-char (code-char b) stream))
     (%write-crlf stream))
    ((stringp value)
     (write-char #\$ stream)
     (princ (length value) stream)
     (%write-crlf stream)
     (write-string value stream)
     (%write-crlf stream))
    ((listp value)
     (write-char #\* stream)
     (princ (length value) stream)
     (%write-crlf stream)
     (dolist (elt value) (encode-resp3 elt stream)))
    (t (error 'cache-error
              :message (format nil "cannot encode ~s as RESP3" value))))
  value)

(defun encode-resp3-to-string (value)
  (with-output-to-string (s)
    (encode-resp3 value s)))

(defun encode-resp3-command (parts)
  "Encode a Redis command (list of strings) as a RESP3 array of bulk strings."
  (encode-resp3-to-string (mapcar #'%utf8 parts)))

(defun %read-line-crlf (stream)
  (let ((line (read-line stream nil :eof)))
    (when (eq line :eof)
      (error 'cache-error :message "unexpected EOF decoding RESP3"))
    (string-right-trim '(#\Return) line)))

(defun %parse-int (s)
  (parse-integer s :junk-allowed t))

(defun decode-resp3 (stream)
  "Read one RESP3 value from STREAM."
  (let* ((line (%read-line-crlf stream))
         (type (if (plusp (length line)) (char line 0)
                   (error 'cache-error :message "empty RESP3 line")))
         (rest (subseq line 1)))
    (ecase type
      (#\+ (make-resp-simple-string rest))
      (#\- (make-resp-error rest))
      (#\: (%parse-int rest))
      (#\$
       (let ((n (%parse-int rest)))
         (if (or (null n) (< n 0))
             :resp-null
             (let ((buf (make-string n)))
               (read-sequence buf stream)
               (%read-line-crlf stream)
               buf))))
      (#\*
       (let ((n (%parse-int rest)))
         (if (or (null n) (< n 0))
             :resp-null
             (loop repeat n collect (decode-resp3 stream)))))
      (#\_ :resp-null)
      (#\#
       (cond
         ((or (string-equal rest "t") (string= rest "t")) :resp-true)
         ((or (string-equal rest "f") (string= rest "f")) :resp-false)
         (t (error 'cache-error :message (format nil "bad RESP3 bool ~s" rest)))))
      (#\,
       (let* ((*read-default-float-format* 'double-float)
              (n (read-from-string rest)))
         (float n 1.0d0)))
      (#\%
       (let ((n (%parse-int rest)))
         (make-resp-map
          (loop repeat n
                collect (cons (decode-resp3 stream) (decode-resp3 stream))))))
      (#\~
       (let ((n (%parse-int rest)))
         (make-resp-set (loop repeat n collect (decode-resp3 stream))))))))

(defun decode-resp3-from-string (string)
  (with-input-from-string (s string)
    (decode-resp3 s)))
