(ns kraken.api.core
  (:use [kraken.model]
        [kraken.api.cryptsy])
  (:require [clojure.core.async :as as]))

(system)

;;; external configuration of cryptsy:
(swap! +system+ #(configure % [:exchanges :cryptsy] (read-string (slurp (str (System/getProperty "user.home") "/.cryptsy/config.edn")))))

(swap! +system+ #(initialize (component % :system) %))
(swap! +system+ #(shutdown (component % :system) %))
(swap! +system+ #(start (component % :system) %))

(shutdown (component @+system+ :system) @+system+)


(switch-context @+system+ :system :up)
(def s (switch-context @+system+ :system :up))
(reset! +system+ s)

(swap! +system+ (switch-context % :system :up))


(def s (initialize (component @+system+ :system) @+system+))
(reset! +system+ s)

(swap! +system+ #(initialize (component % :system) %))

(component @+system+ :system)

(def s (shutdown (component @+system+ :system) @+system+))
(reset! +system+ s)

(def s (switch-context @+system+ :system :down))

(swap! +system+ #(shutdown (component % :system) %))
;; OK

(def s (initialize (component @+system+ :system) @+system+))
(reset! +system+ s)

(swap! +system+ #(initialize (component % :system) %))

(def s (stop (component @+system+ :system) @+system+))
(reset! +system+ s)

(swap! +system+ #(stop (component % :system) %))
;; OK

(swap! +system+ #(initialize (component % :system) %))

(swap! +system+ #(start (component % :system) %))


(system)


(swap! +system+ #(start (component % :system) %))

(as/take! (get-cfg (system) [:exchanges :cryptsy] :doge-trade-channel) (fn [v] (println "Got" v) (flush)))

(swap! +system+ #(stop-component % :system))

(swap! +system+ #(start-component % :system))
;; All ok



(log @+system+ :test "test")
(error @+system+ :test "test")

(shutdown-component @+system+ :system)
(as/close! (system-error-channel @+system+))
;; endless loop

;;; TODO:
;;; - also make component for kraken
;;; - then see what next, maybe make exchanges component, 
;;; - make elastic component (should go in kraken.elastic...)
;;; - combine (in kraken.core)


