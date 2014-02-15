(ns kraken.api.core
  (:use [kraken.model]
        [kraken.api.cryptsy]))

(system)

;;; initial configuration of cryptsy:
(swap! +system+ #(initialize-component
                  (configure % [:exchanges :cryptsy] (read-string (slurp (str (System/getProperty "user.home") "/.cryptsy/config.edn"))))
                  [:exchanges :cryptsy]))
(system)

(def public-key (get-cfg (system) [:exchanges :cryptsy] :public-key))
(def private-key (get-cfg (system) [:exchanges :cryptsy] :private-key))
(def market-id (:id (first (filter #(= (:market-code %) "DOGE/BTC") (get-cfg (system) [:exchanges :cryptsy] :markets)))))
(def exchange-time-zone (get-cfg (system) [:exchanges :cryptsy] :exchange-time-zone))

(market-trades public-key
               private-key
               market-id
               exchange-time-zone)

;;; => TODO:
;;; - make functions to call endpoints without having to fetch keys etc (should go in cryptsy)
;;; - provide channels in cryptsy component
;;; - also make component for kraken
;;; - then see what next, maybe make exchanges component, 
;;; - make elastic component (should go in kraken.elastic...)
;;; - combine (in kraken.core)
