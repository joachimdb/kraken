(ns kraken.channels
  (:require [clojure.core.async :as as]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]))

(defn rec-channel [f x0 interval control-channel error-channel]
  (let [out (as/chan)]
    (as/go 
      (loop [x x0
             status :stop
             [control c] (as/alts! [control-channel (as/timeout interval)])]
        (cond (= c control-channel)
              (recur x control [nil nil])
              (= status :close)
              (as/close! out)
              (= status :start)
              (let [next (try (f x)
                              (catch Exception e e))] 
                (if-not (isa? (type next) java.lang.Exception)
                  (do (as/>! out next)
                      (recur next status (as/alts! [control-channel (as/timeout interval)])))
                  (do (as/>! error-channel next)
                      (recur x status (as/alts! [control-channel (as/timeout interval)])))))
              :else (let [control (as/<! control-channel)]
                      (recur x (or control :close) [nil nil])))))
    out))

;; (def poll-interval 5000)
;; (def control-channel (as/chan))
;; (def error-channel (as/chan))
;; (def rc (rec-channel inc 0 5000 control-channel error-channel))
;; (as/>!! control-channel :start)
;; (as/take! rc (fn [v] (println "Got " v) (flush)))
;; (as/>!! control-channel :close)

;; (as/take! error (fn [v] (println "Error " v) (flush)))
