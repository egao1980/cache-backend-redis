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

(defun start-flaky-server (dispatch drops &key (port 0))
  "A scripted server whose Nth connection is closed after (NTH (1- N) DROPS)
replies; connections past the list are served until the client leaves.
PORT 0 picks an ephemeral port."
  (let* ((listener (usocket:socket-listen "127.0.0.1" port :reuse-address t
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

(defun %hellos (srv)
  (count "HELLO" (ss-commands srv) :key #'second :test #'equal))

(defun %drop-first-connection (c srv)
  "PUT on C, then wait until SRV's first connection has served HELLO + SET
and been closed."
  (cache-protocol:cache-put c "k" "v")
  (%wait-until (lambda () (= 2 (length (ss-commands srv)))))
  (sleep 0.1))

(deftest retries-is-an-upper-bound
  ;; Three drops in a row need three resends; :retries 2 allows two.
  (let ((srv (start-flaky-server (%fake-dispatch) '(2 1 1 nil))))
    (unwind-protect
        (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv)
                             :retries 2 :retry-delay 0)
          (%drop-first-connection c srv)
          (ok (signals (cache-protocol:cache-get c "k")
                'cache-protocol:cache-connection-error)
              ":retries 2 gives up after the third dropped connection")
          (ok (= 3 (%hellos srv)) (format nil "1 + 2 connections (HELLOs: ~D)" (%hellos srv)))
          (ok (equal "v" (cache-protocol:cache-get c "k")) "the next call reconnects"))
      (stop-scripted-server srv))))

(deftest retries-back-off-after-the-first-resend
  ;; Resend 1 at once, resend 2 after RETRY-DELAY, resend 3 after twice that.
  (let ((srv (start-flaky-server (%fake-dispatch) '(2 1 1 nil))))
    (unwind-protect
        (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv)
                             :retries 3 :retry-delay 1/10)
          (%drop-first-connection c srv)
          (let* ((start (get-internal-real-time))
                 (got (cache-protocol:cache-get c "k"))
                 (elapsed (/ (- (get-internal-real-time) start)
                             internal-time-units-per-second)))
            (ok (equal "v" got) "three resends get the reply")
            (ok (>= elapsed 3/10)
                (format nil "waited 0.1 + 0.2 s between resends (~,2F s)" elapsed))))
      (stop-scripted-server srv))))

(deftest a-refused-reconnect-is-retried-until-the-server-is-back
  ;; The server restarts: the connection is dropped, then the port refuses
  ;; connections for 0.2 s, then the server listens again. The backoff
  ;; (0.05, 0.1, 0.2, 0.4 ...) keeps resending until it is back.
  (let* ((dispatch (%fake-dispatch))
         (srv (start-flaky-server dispatch '(2)))
         (port (ss-port srv))
         (again nil))
    (unwind-protect
        (with-redis-cache (c :host "127.0.0.1" :port port :connect-timeout 1/2
                             :retries 6 :retry-delay 1/20)
          (%drop-first-connection c srv)
          (stop-scripted-server srv)
          (let ((restarter (bt2:make-thread
                            (lambda ()
                              (sleep 0.2)
                              (setf again (start-flaky-server dispatch nil :port port)))
                            :name "redis-restart")))
            (let ((got (handler-case (cache-protocol:cache-get c "k")
                         (cache-protocol:cache-connection-error (e) e))))
              (bt2:join-thread restarter)
              (ok (equal "v" got)
                  (format nil "GET waits out the restart (got ~A)" got)))))
      (when again (stop-scripted-server again)))))

(deftest eof-mid-reply-names-the-endpoint
  ;; The decoder's EOF error is re-signalled with host:port.
  (let ((srv (start-flaky-server (%fake-dispatch) '(1 1))))
    (unwind-protect
        (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :retries 0)
          (let ((e (handler-case (progn (cache-protocol:cache-put c "k" "v")
                                   (cache-protocol:cache-get c "k")
                                   nil)
                     (cache-protocol:cache-connection-error (e) e))))
            (ok e "a dropped connection signals cache-connection-error")
            (ok (and e (search (format nil "127.0.0.1:~D" (ss-port srv))
                               (princ-to-string e)))
                (format nil "the report names the endpoint: ~A" e))))
      (stop-scripted-server srv))))
