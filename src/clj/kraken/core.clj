(ns kraken.core
  (:use [kraken.system]
        [kraken.model]
        [kraken.elastic]
        [kraken.api.cryptsy]
        [compojure.core]
        [ring.util.serve]) 
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [kraken.channels :as ch]
            [clojure.core.async :as as]))

(defcomponent :app [:elastic :cryptsy]
  ComponentP 
  (initial-config [this] {})
  (initialize [this system] 
              (create-index! system :elastic (index-name system "cryptsy"))
              (as/go-loop []
                (let [t (as/<! (get-cfg system :cryptsy :trade-channels "DOGE/BTC"))]
                  (when t
                    (as/>! (get-cfg system :elastic :index-channel) t)
                    (recur))))
              system)
  (start [this system] system)
  (stop [this system] system)
  (shutdown [this system]))

(start!)

;; (def ts (trades (system) "cryptsy"
;;                 :sort {:time {:order "desc"}}
;;                 :size 1000))

;; (shutdown!)



;; ;; defroutes macro defines a function that chains individual route
;; ;; functions together. The request map is passed to each function in
;; ;; turn, until a non-nil response is returned.
;; (defroutes app-routes
;;   ; to serve document root address
;;   (GET "/" [] "<p>Hello from compojure</p>")
;;   ; to serve static pages saved in resources/public directory
;;   (route/resources "/")
;;   ; if page is not found
;;   (route/not-found "Page not found"))

;; ;; site function creates a handler suitable for a standard website,
;; ;; adding a bunch of standard ring middleware to app-route:
;; (def handler
;;   (handler/site app-routes))

;; (serve-headless handler)


