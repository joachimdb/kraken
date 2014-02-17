(ns kraken.api.core
  (:use [kraken.model]
        [kraken.api.cryptsy])
  (:require [clojure.core.async :as as]))

(system)

;;; external configuration of cryptsy:
(swap! +system+ #(configure % [:exchanges :cryptsy] (read-string (slurp (str (System/getProperty "user.home") "/.cryptsy/config.edn")))))

(swap! +system+ #(initialize (component % :system) %))
(system)

(swap! +system+ #(start-component % :system))

(as/take! (get-cfg (system) [:exchanges :cryptsy] :doge-trade-channel) (fn [v] (println "Got" v) (flush)))

(swap! +system+ #(stop-component % :system))

(swap! +system+ #(start-component % :system))
;; All ok

(swap! +system+ #(shutdown-component % :system))
;; endless loop

;;; TODO:
;;; - also make component for kraken
;;; - then see what next, maybe make exchanges component, 
;;; - make elastic component (should go in kraken.elastic...)
;;; - combine (in kraken.core)


