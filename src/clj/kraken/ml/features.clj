(ns kraken.ml.features
  (:use [kraken.model]
        [kraken.system]
        [kraken.view.charts])
  (:require [incanter.core :as inc]))

;; A data point is characterized by features price, volume, momentum, accelaration, ...
;; These can be computed at different resolutions (time scales)
;; Furthermore, it's probably not really the price that is most informative, but the deviation from the value at the next time scale

;; Thus we start with computing a price curve at the largest resolution

(def ts (trades (system) "cryptsy"))

(meta (trades (system) :search_type "count"))
(count (price-curve (system) "5m"))
(volume-curve (system) "5m")

(inc/view (price-chart (price-curve (system) "1m")))
(inc/view (price-chart (price-curve (system) "15m")))

