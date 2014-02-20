(ns kraken.view.charts
  (:use [kraken.model]
        [kraken.elastic]
        [incanter.core]
        [incanter.charts])
  (:require [clj-time.core :as tcore]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]))

(defn price-chart [price-curve]
  (let [times (map #(tcoerce/to-long (:time %)) price-curve)
        ch (time-series-plot times
                             (map :mean price-curve)
                             :legend true
                             :series-label "mean"
                             :x-label "Time"
                             :y-label "Price")]
    (doseq [key [:min :max]]
      (add-lines ch times (map key price-curve)
                 :series-label (name key)))
    ch))

