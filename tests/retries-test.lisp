(in-package #:cache-backend-redis/tests)

;;; Evidence against PR #6 (fix for #3): :RETRIES. The README says a
;;; dropped socket "is retried transparently when the connection had been
;;; up (up to :retries times)", and MAKE-REDIS-CACHE-BACKEND's docstring
;;; "reconnects and resends up to RETRIES times". %CALL decides per attempt
;;; with WAS-CONNECTED, read before %ENSURE-STREAM opens the new
;;; connection, so every attempt after the first starts "not connected" and
;;; its failure propagates. :retries 3 behaves like :retries 1.

(defun %serve-flaky (server sock dispatch drop-after)
  "Serve SOCK with DISPATCH; close it after DROP-AFTER replies (NIL: never)."
  (let ((stream (usocket:socket-stream sock))
        (served 0))
    (unwind-protect
         (handler-case
             (loop
               (when (and drop-after (>= served drop-after))
                 (return))
               (let ((parts (decode-resp3 stream)))
                 (bt2:with-lock-held ((ss-lock server))
                   (push (cons served parts) (ss-log server)))
                 (encode-resp3 (funcall dispatch parts) stream)
                 (force-output stream)
                 (incf served)))
           ((or cache-protocol:cache-error stream-error usocket:socket-error) () nil))
      (usocket:socket-close sock))))

(defun start-flaky-server (dispatch drops)
  "A scripted server whose Nth connection is closed after (NTH (1- N) DROPS)
replies; connections past the list are served until the client leaves."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                        :element-type '(unsigned-byte 8)))
         (server (make-scripted-server :listener listener
                                       :port (usocket:get-local-port listener))))
    (setf (ss-thread server)
          (bt2:make-thread
           (lambda ()
             (loop with conn = 0
                   until (ss-stopping server)
                   when (usocket:wait-for-input listener :timeout 0.05 :ready-only t)
                     do (%serve-flaky server (usocket:socket-accept listener)
                                      dispatch (nth (1- (incf conn)) drops))))
           :name "flaky-redis"))
    server))

(deftest retries-3-resends-past-two-dropped-connections
  ;; Connection 1: HELLO and SET, then closed (an idle connection the
  ;; server dropped). Connection 2: HELLO, then closed (the server is
  ;; restarting). Connection 3 works. Two resends are needed, and
  ;; :retries 3 allows three.
  (let ((srv (start-flaky-server (%fake-dispatch) '(2 1 nil))))
    (unwind-protect
         (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :retries 3)
           (cache-protocol:cache-put c "k" "v")
           (%wait-until (lambda () (= 2 (length (ss-commands srv)))))
           (sleep 0.1)
           (let ((got (handler-case (cache-protocol:cache-get c "k")
                        (cache-protocol:cache-connection-error (e) e))))
             (ok (equal "v" got)
                 (format nil ":retries 3 survives two drops in a row (got ~A)" got))
             (ok (= 3 (count "HELLO" (ss-commands srv) :key #'second :test #'equal))
                 (format nil "three connections were opened (HELLOs: ~D)"
                         (count "HELLO" (ss-commands srv) :key #'second :test #'equal)))))
      (stop-scripted-server srv))))
