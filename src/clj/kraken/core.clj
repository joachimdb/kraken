(ns kraken.core
  (:use [kraken.system]
        [kraken.elastic]
        [kraken.api.cryptsy]
        [kraken.model]
        ; [kraken.api.core]
        [compojure.core]
        [ring.util.serve]) 
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
            [compojure.handler :as handler]
            [compojure.route :as route]
            ; [kraken.elastic :as es]
            [kraken.channels :as ch]
            ; [kraken.api.public :as pub]
            [clojure.core.async :as as]))

;; (hard-reset!)
(system)
(initialize!)
(start!)
(shutdown!)

;; TODO: the following worked, but I didn't notice anything
(create-index! (system) (index (mk-trade "cryptsy" "DOGE/BTC" 0 0 0) (system)))

(def t (as/<!! (get-cfg (system) :cryptsy :trade-channels "DOGE/BTC")))
(as/>!! (get-cfg (system) :elastic :index-channel) t)

(as/go-loop []
  (let [t (as/<! (get-cfg (system) :cryptsy :trade-channels "DOGE/BTC"))]
    (when t
      (info (system) :control "indexing trade")
      (as/>! (get-cfg (system) :elastic :index-channel) t)
      (recur))))

(trades (system) "cryptsy"
        :sort {:time {:order "desc"}})

(tcore/now)



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




;; (comment 
  
  
;;   [(get-in (meta (es/ticks (es/local-connection) :search_type "count")) [:total]) 
;;    (get-in (meta (es/trades (es/local-connection) :search_type "count")) [:total])
;;    (get-in (meta (es/spreads (es/local-connection) :search_type "count")) [:total])]

;;   [128962 13906 51974]




;;   (into {} (for [a ["XLTCZEUR" "XNMCZEUR" "XXBTZEUR"]]
;;              (vector a 
;;                      (select-keys (first (es/ticks (es/local-connection)
;;                                                    :query {:match {:asset a}}
;;                                                    :sort [{:time {:order "desc"}}] 
;;                                                    :size 1))
;;                                   [:last-price :time]))))

;;   {"XLTCZEUR"
;;    {:time #<DateTime 2013-12-12T09:08:51.000Z>, :last-price 22.77569},
;;    "XNMCZEUR"
;;    {:time #<DateTime 2013-12-12T09:08:51.000Z>, :last-price 4.685},
;;    "XXBTZEUR"
;;    {:time #<DateTime 2013-12-12T09:08:51.000Z>, :last-price 636.00004}}


;;   (into {} (for [a ["XLTCZEUR" "XNMCZEUR" "XXBTZEUR"]]
;;              (vector a 
;;                      (select-keys (first (es/trades (es/local-connection)
;;                                                    :query {:match {:asset a}}
;;                                                    :sort [{:time {:order "desc"}}] 
;;                                                    :size 1))
;;                                   [:price :time]))))

;;   {"XLTCZEUR" {:time #<DateTime 2013-12-12T07:13:40.000Z>, :price 22.77569},
;;    "XNMCZEUR" {:time #<DateTime 2013-12-12T07:08:06.000Z>, :price 4.685},
;;    "XXBTZEUR" {:time #<DateTime 2013-12-12T10:56:17.000Z>, :price 646.31984}}

;;   (def print-sink (ch/print-sink))

;;   ;;; (dis)connecting to the filtered tick source:
;;   (def print-tick-connection (connect! (:source es-tick-connection)
;;                                        print-sink
;;                                        (as/filter> (fn [tick] (println "foo") (= "XLTCZEUR" (:asset tick))) (as/chan))))
  
;;   (def print-tick-connection (connect! (:source es-tick-connection)
;;                                        print-sink
;;                                        (as/map< (fn [tick] (println "bar") tick)
;;                                                 (as/filter> (fn [tick] (println "foo") (= "XLTCZEUR" (:asset tick))) (as/chan)))))

  
;;   (/ (- 252.0 14) 252.0)
;;   (disconnect! print-tick-connection)
  
;;   (reset! continue false)
;;   (def continue (atom true))
;;   (def s (let [out (as/chan)]
;;            (as/go-loop []
;;               (when @continue
;;                 (as/put! out (rand-int 10))
;;                 (as/<! (as/timeout 1000))
;;                 (recur)))
;;            out))
;;   (def p (let [in (as/chan)]
;;            (as/go-loop []
;;               (let [v (as/<! in)]
;;                 (when-not (nil? v)
;;                   (recur))))
;;            in))
;;   (as/pipe (as/map< (fn [x] (println "<") (flush) x) s) 
;;            (as/map> (fn [x] (println ">") (flush) x) p))
;;   )
