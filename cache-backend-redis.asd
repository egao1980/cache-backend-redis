(defsystem "cache-backend-redis"
  :version "0.2.0"
  :description "RESP3 Redis/Valkey backend for cache-protocol (host/port or redis:// URL with AUTH, SELECT, TLS; or an injected stream / send-fn)"
  :author "egao1980"
  :license "MIT"
  :depends-on ((:version "cache-protocol" "0.2.0") (:version "encoding-protocol" "0.3.0")
               "flexi-streams"
               "usocket" (:version "quri" "0.7.2") "cl-stack-ssl" "bordeaux-threads")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "resp3")
               (:file "connection")
               (:file "backend"))
  :in-order-to ((test-op (test-op "cache-backend-redis/tests"))))

(defsystem "cache-backend-redis/tests"
  :depends-on ("cache-backend-redis" "encoding-protocol" "flexi-streams" "rove" "usocket"
               "bordeaux-threads")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "resp3-test")
               (:file "backend-test")
               (:file "wire-test")
               (:file "connect-test")
               (:file "connection-test")
               (:file "live-test")
               (:file "retries-test")
               (:file "evidence-r2-test")
               (:file "round3-test")
               (:file "evidence-r3-test")
               (:file "round4-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
