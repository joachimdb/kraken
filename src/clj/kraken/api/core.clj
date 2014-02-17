(ns kraken.api.core
  (:use [kraken.model]
        [kraken.api.cryptsy])
  (:require [clojure.core.async :as as]))

(system)

;;; external configuration of cryptsy:
(swap! +system+ #(configure % [:exchanges :cryptsy] (read-string (slurp (str (System/getProperty "user.home") "/.cryptsy/config.edn")))))

;;; Doesn't work because this first tries to initialize dependent components but error and log channel are not yet in place
;;; (swap! +system+ #(initialize-component % :system))

(swap! +system+ #(initialize (component % :system) %))
(swap! +system+ #(shutdown (component % :system) %))
;; OK

(swap! +system+ #(initialize (component % :system) %))
(swap! +system+ #(stop (component % :system) %))
;; OK

@+system+
(initialize (component @+system+ :system) @+system+)
(swap! +system+ #(initialize (component % :system) %))
;; => exception: system not initialized
;; Reason: initialize sets error and log channels, then calls initialize-component on :system which is in status :stopped, this will first call shutdown, which will again close and remove log and error channels

;; => what if in init/start/stop/shutdown fn of system component we handle dependencies directly instead of going through initialize etc?


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


