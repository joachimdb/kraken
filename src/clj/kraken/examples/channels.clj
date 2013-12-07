(ns kraken.examples.channels
  (:use [kraken.channels]
        [kraken.model])
  (:require [clojure.core.async :as as]
            [kraken.elastic :as es]))


;;; prints the value 1.2345 ten times:
(let [cc (repeat-channel 10 1.2345)]
  (as/go (loop [next (as/<! cc)]
           (println "received" next)
           (when next
             (recur (as/<! cc)))))))


;;; prints ten random values with pauses in between:
(let [cc (repeatedly-channel 10 1000 (fn [] (rand-int 100)))]
  (as/go (loop [next (as/<! cc)]
           (println "received" next)
           (when next
             (recur (as/<! cc))))))

;;; prints the values true ten times with pauses in between:
(let [cc (pulse 10 1000)]
  (as/go (loop [next (as/<! cc)]
           (println "received" next)
           (when next
             (recur (as/<! cc))))))

;;; prints the values 1.2345 and true ten times with pauses in between:
(let [cc (interleave-channels (repeat-channel 10 1.2345)
                              (pulse 10 1000))]
  (as/go (loop [next (as/<! cc)]
           (println "received" next)
           (when next
             (recur (as/<! cc))))))

;;; accumulating the values:
(let [cc (repeat-channel 10 1.2345)
      acc (accumulator cc)]
  (Thread/sleep 1000) ;; give some time to accumulate the values
  @acc)

;;; ticker channel test

(let [cc (tick-channel "LTCEUR" 5000)]
  (reset! +continue+ true)
  (as/go (loop [next (as/<! cc)]
           (when next
             (println next)
             (recur (as/<! cc))))))
(reset! +continue+ true)
(reset! +continue+ false)

;;; Sink tests:

;;; prints 1.2345 ten times
(let [cc (repeat-channel 10 1.2345)
      pc (print-sink)]
  (as/admix pc cc))

;;; Same but with newlines in between:
(let [cc (interleave-channels (repeat-channel 10 1.2345)
                              (repeat-channel "\n"))
      pc (print-sink)]
  (as/admix pc cc))

;;; Same plus prefix:
(let [cc (interleave-channels (repeat-channel 10 1.2345)
                              (repeat-channel "\n"))
      pc (print-sink)]
  (as/admix pc (as/map< #(str "prefix: " %) cc)))

;;; Randomly distribute over two channels:
(let [tc (repeatedly-channel 10 nil #(rand-int 100))
      ps1 (print-sink)
      ps2 (print-sink)]
  (as/admix ps1 (as/map< #(str "sink1: " % "\n") tc))
  (as/admix ps2 (as/map< #(str "sink2: " % "\n") tc)))

;;; Source tests

;;; prints 1.2345 ten times
(let [cs (constant-source 10 1.2345)
      ps (print-sink)
      chan (as/chan)]
  (as/tap cs chan)
  (as/admix ps chan))

;;; Connect test
(let [cs (constant-source 10 1.2345)
      ps (print-sink)]
  (connect! cs (print-sink)))

;;; dynamic source tests
(let [cs (dynamic-source 10 nil (fn [] (str (rand-int 100) "\n")))
      ps (print-sink)
      chan (as/chan)]
  (as/tap cs chan)
  (as/admix ps chan))

;;; print and collect:
(let [ds (dynamic-source 10 nil (fn [] (str (rand-int 100) "\n")))
      ps (print-sink)
      ch (as/chan)
      acc (accumulator ch)]
  (as/tap ds ch)
  (connect! ds ps)
  (Thread/sleep 1000) ;; give some time to accumulate the values
  @acc)

;;; ticker test

(reset! +continue+ true)
(let [ts (tick-source "LTCEUR" 1000)
      ps (print-sink)]
  (connect! ts (print-sink)))
(reset! +continue+ false)

;;; tick indexer test
(es/delete-indices  (es/local-connection) [:tick])
(es/create-indices (es/local-connection) [:tick])
(reset! +continue+ true)
(connect! (tick-source "LTCEUR" 5000)
          (tick-indexer (es/local-connection)))
(reset! +continue+ false)
(es/ticks)

;;;; put ticks into elastic
(es/init-indices (es/local-connection))
(reset! +continue+ true)
(let [ticks (ticker-source "LTCEUR,NMCEUR,BTCEUR" 5000)
      es-tick-indexer (es/ticker-sink (es/local-connection))
      ps (print-sink)]
  (connect! ticks ps)
  (connect! ticks es-tick-indexer))
(reset! +continue+ false)
(toggle-print-sink-print)



;; (reset! +continue+ false)





