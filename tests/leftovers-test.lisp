(in-package #:cache-backend-redis/tests)

;;; cache-backend-redis#11: leftovers of the 0.2.1 adversarial check.

;;; --- %REDACT-URL: a URL with no :// is masked past its scheme -------------

(deftest-parametrize a-url-without-an-authority-shows-no-password
    ((url)
     ("redis:/:WORD-leftover")
     ("redis::WORD-leftover")
     ("redis:WORD-leftover@host"))
  (let ((text (handler-case (progn (parse-redis-url url) "no error")
                (cache-protocol:cache-error (e) (princ-to-string e)))))
    (ok (string/= "no error" text) (format nil "~A is refused" url))
    (ok (not (search "WORD-leftover" text))
        (format nil "no part of the password in: ~A" text))))

;;; --- A caller's channel with options it cannot use -----------------------

(defun %caller-stream ()
  (make-two-way-stream (flexi-streams:make-in-memory-input-stream #())
                       (flexi-streams:make-in-memory-output-stream)))

(deftest-parametrize a-caller-channel-is-refused-next-to-connection-options
    ((args)
     ((list :stream (%caller-stream) :tls t))
     ((list :stream (%caller-stream) :password "leftover-PW"))
     ((list :stream (%caller-stream) :db 3))
     ((list :stream (%caller-stream) :username "app"))
     ((list :stream (%caller-stream) :port 6380))
     ((list :stream (%caller-stream) :client-name "app"))
     ((list :stream (%caller-stream) :tls-verify :none))
     ((list :send-fn #'identity :password "leftover-PW"))
     ((list :send-fn #'identity :db 1))
     ((list :stream (%caller-stream) :send-fn #'identity)))
  (let ((e (handler-case (progn (apply #'make-redis-cache-backend args) nil)
             (cache-protocol:cache-error (e) e))))
    (ok (typep e 'cache-protocol:cache-error) (format nil "~S refused: ~A" (remove-if #'streamp args) e))
    (ok (not (and e (search "leftover-PW" (princ-to-string e)))) "no password in the report")))

(deftest a-caller-stream-alone-is-still-accepted
  (ok (typep (make-redis-cache-backend :stream (%caller-stream)) 'redis-cache-backend))
  (ok (typep (make-redis-cache-backend :send-fn #'identity :retries 0) 'redis-cache-backend)))

;;; --- Ratio values: no exact decimal text, so refused ---------------------

(deftest-parametrize a-ratio-or-complex-value-is-refused
    ((value)
     (1/3)
     (#c(1 2)))
  (multiple-value-bind (c fake sent) (%recording-cache)
    (declare (ignore fake))
    (let ((e (handler-case (progn (cache-protocol:cache-put c "k" value) nil)
               (cache-protocol:cache-error (e) e))))
      (ok (typep e 'cache-protocol:cache-error) (format nil "cache-put ~S → cache-error (~A)" value e))
      (ok (null (funcall sent)) "nothing sent"))
    (let ((e (handler-case (progn (cache-protocol:cache-cas c "k" nil value) nil)
               (cache-protocol:cache-error (e) e))))
      (ok (typep e 'cache-protocol:cache-error) (format nil "cache-cas ~S → cache-error (~A)" value e)))))

;;; --- Redis 5: a username without a password fails at connect -------------

(deftest redis5-username-without-a-password-fails-at-connect
  ;; redis-py fails at connect. AUTH app "" (arity refused) then AUTH "".
  (with-scripted-server (srv (%redis5-dispatch (make-fake-redis) "pw"))
    (with-redis-cache (c :host "127.0.0.1" :port (ss-port srv) :username "app")
      (let ((e (handler-case (progn (cache-protocol:cache-get c "k") nil)
                 (cache-protocol:cache-error (e) e))))
        (ok (and e (search "AUTH" (princ-to-string e)))
            (format nil "an AUTH error at connect (got ~A)" e))))
    (ok (equal '(("HELLO" "3" "AUTH" "app" "") ("AUTH" "app" "") ("AUTH" ""))
               (ss-commands srv 1))
        (format nil "wire: ~S" (ss-commands srv 1)))))

(deftest live-redis5-username-without-a-password-fails-at-connect
  (let ((port (%env-port "CACHE_REDIS_TEST_REDIS5_PORT")))
    (if (null port)
        (skip "no Redis 5 (CACHE_REDIS_TEST_REDIS5_PORT)")
        (let ((text (handler-case
                        (with-redis-cache (c :host (%live-host) :port port :username "app")
                          (cache-protocol:cache-get c "leftover-r5")
                          "no error")
                      (cache-protocol:cache-error (e) (princ-to-string e)))))
          (ok (search "AUTH" text)
              (format nil "Redis 5, :username with no :password → AUTH error (got ~S)" text))
          (ok (not (search "NOAUTH" text)) "not NOAUTH from the first command")))))
