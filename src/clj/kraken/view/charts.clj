(ns kraken.view.charts
  (:use [kraken.model]
        [kraken.elastic]
        [incanter.core]
        [incanter.charts])
  (:require [clj-time.core :as tcore]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]))

(defn price-chart 
  ([ticks] (price-chart ticks :bid-price :ask-price :last-price))
  ([ticks & price-keys]
     (when-not (empty? price-keys)
       (let [times (map #(tcoerce/to-long (:time %)) ticks)
             ch (time-series-plot times
                                  (map (first price-keys) ticks)
                                  :legend true
                                  :series-label (name (first price-keys))
                                  :x-label "Time"
                                  :y-label (:asset (first ticks)))]
         (doseq [key (rest price-keys)]
           (add-lines ch times (map key ticks)
                      :series-label (name key)))
         ch))))


    
