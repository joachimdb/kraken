(ns kraken.modules
  (:use [kraken.channels])
  (:require [clojure.core.async :as as]
            [kraken.elastic :as es]))

(defn monitor [channel]
  (as/admix (print-sink) channel))

