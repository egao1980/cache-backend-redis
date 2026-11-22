(defsystem "cache-backend-redis"
  :version "0.1.1"
  :description "RESP3 Redis/Valkey backend for cache-protocol (injectable binary stream / send-fn)"
  :author "egao1980"
  :license "MIT"
  :depends-on ("cache-protocol" "encoding-protocol" "flexi-streams")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "resp3")
               (:file "backend"))
  :in-order-to ((test-op (test-op "cache-backend-redis/tests"))))

(defsystem "cache-backend-redis/tests"
  :depends-on ("cache-backend-redis" "encoding-protocol" "flexi-streams" "rove")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "resp3-test")
               (:file "backend-test")
               (:file "wire-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
