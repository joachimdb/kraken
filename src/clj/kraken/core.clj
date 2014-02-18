(ns kraken.core
  (:use [kraken.system]
        [kraken.elastic]
        [kraken.model]
        [kraken.api.core]
        [compojure.core]
        [ring.util.serve]) 
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [compojure.handler :as handler]
            [compojure.route :as route]
            ; [kraken.elastic :as es]
            [kraken.channels :as ch]
            ; [kraken.api.public :as pub]
            [clojure.core.async :as as]))

(system)

;;; external configuration of cryptsy:
(configure! :cryptsy (read-string (slurp (str (System/getProperty "user.home") "/.kraken/cryptsy/config.edn"))))
(configure! :elastic (read-string (slurp (str (System/getProperty "user.home") "/.kraken/elastic/config.edn"))))
(system)

(initialize!)
(shutdown!)
(start!)
(stop!)

;;; TODO: 
;;; - make full reset work and call it automatically when call on failed system
;;; - figure out why shutdown throws exception (prob bec tries to call protocol method on non-existant instance)
;;; - make fresh components in full-reset
;;; - also make component for kraken
;;; - then see what next, maybe make exchanges component, 
;;; - make elastic component (should go in kraken.elastic...)
;;; - combine (in kraken.core)





;; "controler" => actual call to first initialize / start in the chain...

(initialize (get-in (system) [:components :system :instance]))

;; => should initialize all defined components


(go) => calls 
(swap! +config+ (api/initialize {:api :cryptsy))
(cfg! [:api] )
;; ;;; See file kraken.examples.channels.clj for examples

;;  ;; (es/delete-indices (es/local-connection))
;; (es/create-indices (es/local-connection))
;; (reset! ch/+continue+ true)
;; (def poll-interval (* 1000 60 3)) ;; 3 minutes
;; ;; (reset! ch/+continue+ false)

;; (def +pairs+ ["XLTCZEUR" "XNMCZEUR" "XXBTZEUR"])

;; (def es-tick-connection 
;;   (connect! (ch/tick-source (apply str (interpose "," +pairs+)) poll-interval false)
;;             (ch/tick-indexer (es/local-connection))))

;; (defn compute-last-spread-settings [es-connection pair]
;;   (let [new-spreads (pub/spread pair)
;;         indexed-spreads (into #{} (es/all-spreads es-connection
;;                                                   :query (es/match-query :asset pair)
;;                                                   :filter (es/daterange-filter :time (:time (first new-spreads)))))
;;         missing (clojure.set/difference (into #{} new-spreads) indexed-spreads)]
;;     (when-not (empty? missing)
;;       (println "indexing" (count missing) "spreads")
;;       (apply es/index-spread es-connection missing))
;;     ;; (doseq [s (clojure.set/difference (into #{} new-spreads) indexed-spreads)]
;;     ;;   (es/index-spread es-connection s))
;;     {:last (:last (meta new-spreads))
;;      :last-processed (count (filter #(= (* 1000 (:last (meta new-spreads))) (tcoerce/to-long (:time %))) new-spreads))}))

;; (def es-spread-connection
;;   (let [es-connection (es/local-connection)]
;;     (doseq [pair +pairs+]
;;       (let [last-settings (compute-last-spread-settings es-connection pair)]
;;         (connect! (ch/spread-source pair poll-interval false (:last last-settings) (:last-processed last-settings))
;;                   (ch/spread-indexer es-connection))))))

;; (defn compute-last-trade-settings [es-connection pair]
;;   (let [new-trades (pub/trades pair)
;;         indexed-trades (into #{} (es/all-trades es-connection
;;                                                 :query (es/match-query :asset pair)
;;                                                 :filter (es/daterange-filter :time (:time (first new-trades)))))
;;         missing (clojure.set/difference (into #{} new-trades) indexed-trades)]
;;     (when-not (empty? missing)
;;       (println "indexing" (count missing) "trades")
;;       (apply es/index-trade es-connection missing))
;;     {:last (:last (meta new-trades))}))

;; (def es-trade-connection 
;;   (let [es-connection (es/local-connection)]
;;     (doseq [pair +pairs+]
;;       (let [last-settings (compute-last-trade-settings es-connection pair)]
;;         (println "lts:" last-settings)
;;         (connect! (ch/trade-source pair poll-interval false (:last last-settings))
;;                   (ch/trade-indexer (es/local-connection)))))))


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
