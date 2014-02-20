(ns kraken.ml.features
  (:use [kraken.model]
        [kraken.system]
        [kraken.view.charts])
  (:require [incanter.core :as inc]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat]))

(meta (trades (system) :search_type "count"))
;; 1745

(inc/view (price-chart (price-curve (system) "1m")))
(inc/view (price-chart (price-curve (system) "5m")))

(def st (tcore/ago (tcore/hours 1))) ;; 9:49:54
(def et (tcore/now)) ;; 10:49:55


(def pc1 (price-curve (system) "5m"
                      :filter 
                      {:range {:time {:from st :to et}}}))

(meta (trades (system) :search_type "count" 
              :filter {:range {:time {:from (:time (last pc1))}}}))

{:max_score 0.0,
 :total 1,
 :took 2,
 :timed_out false,
 :_shards {:total 1, :successful 1, :failed 0}}

(take 2 (reverse pc1))

({:time #<DateTime 2014-02-20T13:10:00.000Z>,
  :count 2,
  :min 2.1E-6,
  :max 2.11E-6,
  :total 4.21E-6,
  :total_count 2,
  :mean 2.105E-6}
 {:time #<DateTime 2014-02-20T13:05:00.000Z>,
  :count 11,
  :min 2.1E-6,
  :max 2.11E-6,
  :total 2.3120000000000002E-5,
  :total_count 11,
  :mean 2.101818181818182E-6})

(def pc2 (price-curve (system) "5m"
                      :filter 
                      {:range {:time {:from st 
                                      :to et}}}))



(take 2 (reverse pc2))
({:time #<DateTime 2014-02-20T11:00:00.000Z>,
  :count 3,
  :min 2.12E-6,
  :max 2.14E-6,
  :total 6.39E-6,
  :total_count 3,
  :mean 2.13E-6}
 {:time #<DateTime 2014-02-20T10:55:00.000Z>,
  :count 13,
  :min 2.13E-6,
  :max 2.14E-6,
  :total 2.7749999999999997E-5,
  :total_count 13,
  :mean 2.1346153846153844E-6})

;;; => for training we should use the entry



(inc/view (price-chart pc1))
(inc/view (price-chart pc2))


(inc/view (price-chart (price-curve (system) "5m"
                                    :filter 
                                    {:range {:time {:from (tcoerce/to-long (tcore/ago (tcore/hours 2)))
                                                    :to (tcoerce/to-long n)}}})))


;; A data point is characterized by features price, volume, momentum, accelaration, ...
;; These can be computed at different resolutions (time scales)
;; Furthermore, it's probably not really the price that is most informative, but the deviation from the value at the next time scale

;; NOTE: must take care that the features for a point are computed as if they are the final point

;; Thus we start with computing a price curve at two resolutions
(def n (tcore/now))
(def b (tcore/ago (tcore/days 1)))

(def pc1 (rest (price-curve (system) "5m" :filter {:range {:time {:from (tcoerce/to-long b)
                                                                  :to (tcoerce/to-long n)}}})))
(def pc2 (rest (price-curve (system) "2m" :filter {:range {:time {:from (tcoerce/to-long b)
                                                                  :to (tcoerce/to-long n)}}})))

(meta (trades (system) :search_type "count"))
{:max_score 0.0,
 :total 1125,
 :took 5,
 :timed_out false,
 :_shards {:total 1, :successful 1, :failed 0}}
({:time 1392850500000,
  :count 8,
  :min 2.16E-6,
  :max 2.16E-6,
  :total 1.728E-5,
  :total_count 8,
  :mean 2.16E-6}
 {:time 1392850800000,
  :count 12,
  :min 2.15E-6,
  :max 2.17E-6,
  :total 2.591E-5,
  :total_count 12,
  :mean 2.1591666666666666E-6}
 {:time 1392851100000,
  :count 13,
  :min 2.15E-6,
  :max 2.16E-6,
  :total 2.8010000000000005E-5,
  :total_count 13,
  :mean 2.154615384615385E-6}
 {:time 1392851400000,
  :count 10,
  :min 2.16E-6,
  :max 2.17E-6,
  :total 2.162E-5,
  :total_count 10,
  :mean 2.162E-6}
 {:time 1392851700000,
  :count 11,
  :min 2.15E-6,
  :max 2.17E-6,
  :total 2.379E-5,
  :total_count 11,
  :mean 2.162727272727273E-6}
 {:time 1392852000000,
  :count 10,
  :min 2.15E-6,
  :max 2.16E-6,
  :total 2.154E-5,
  :total_count 10,
  :mean 2.154E-6}
 {:time 1392852300000,
  :count 22,
  :min 2.1E-6,
  :max 2.16E-6,
  :total 4.668999999999998E-5,
  :total_count 22,
  :mean 2.1222727272727262E-6}
 {:time 1392852600000,
  :count 16,
  :min 2.12E-6,
  :max 2.14E-6,
  :total 3.405000000000001E-5,
  :total_count 16,
  :mean 2.1281250000000005E-6}
 {:time 1392852900000,
  :count 15,
  :min 2.11E-6,
  :max 2.13E-6,
  :total 3.18E-5,
  :total_count 15,
  :mean 2.12E-6}
 {:time 1392853200000,
  :count 15,
  :min 2.13E-6,
  :max 2.15E-6,
  :total 3.2050000000000007E-5,
  :total_count 15,
  :mean 2.1366666666666672E-6}
 {:time 1392853500000,
  :count 11,
  :min 2.12E-6,
  :max 2.14E-6,
  :total 2.344E-5,
  :total_count 11,
  :mean 2.1309090909090908E-6}
 {:time 1392853800000,
  :count 9,
  :min 2.12E-6,
  :max 2.14E-6,
  :total 1.916E-5,
  :total_count 9,
  :mean 2.1288888888888886E-6}
 {:time 1392854100000,
  :count 8,
  :min 2.11E-6,
  :max 2.13E-6,
  :total 1.7E-5,
  :total_count 8,
  :mean 2.125E-6}
 {:time 1392854400000,
  :count 8,
  :min 2.12E-6,
  :max 2.14E-6,
  :total 1.7069999999999998E-5,
  :total_count 8,
  :mean 2.1337499999999997E-6}
 {:time 1392854700000,
  :count 13,
  :min 2.13E-6,
  :max 2.14E-6,
  :total 2.7749999999999997E-5,
  :total_count 13,
  :mean 2.1346153846153844E-6}
 {:time 1392855000000,
  :count 14,
  :min 2.1E-6,
  :max 2.14E-6,
  :total 2.9700000000000004E-5,
  :total_count 14,
  :mean 2.1214285714285717E-6}
 {:time 1392855300000,
  :count 10,
  :min 2.11E-6,
  :max 2.12E-6,
  :total 2.1129999999999996E-5,
  :total_count 10,
  :mean 2.1129999999999995E-6}
 {:time 1392855600000,
  :count 10,
  :min 2.11E-6,
  :max 2.12E-6,
  :total 2.116E-5,
  :total_count 10,
  :mean 2.116E-6}
 {:time 1392855900000,
  :count 8,
  :min 2.11E-6,
  :max 2.12E-6,
  :total 1.6930000000000002E-5,
  :total_count 8,
  :mean 2.1162500000000003E-6}
 {:time 1392856200000,
  :count 12,
  :min 2.11E-6,
  :max 2.13E-6,
  :total 2.544E-5,
  :total_count 12,
  :mean 2.12E-6}
 {:time 1392856500000,
  :count 7,
  :min 2.12E-6,
  :max 2.14E-6,
  :total 1.491E-5,
  :total_count 7,
  :mean 2.13E-6}
 {:time 1392856800000,
  :count 10,
  :min 2.12E-6,
  :max 2.14E-6,
  :total 2.1300000000000003E-5,
  :total_count 10,
  :mean 2.1300000000000004E-6}
 {:time 1392857100000,
  :count 12,
  :min 2.13E-6,
  :max 2.16E-6,
  :total 2.5760000000000004E-5,
  :total_count 12,
  :mean 2.146666666666667E-6}
 {:time 1392857400000,
  :count 16,
  :min 2.11E-6,
  :max 2.16E-6,
  :total 3.436E-5,
  :total_count 16,
  :mean 2.1475E-6}
 {:time 1392857700000,
  :count 7,
  :min 2.13E-6,
  :max 2.15E-6,
  :total 1.4989999999999999E-5,
  :total_count 7,
  :mean 2.141428571428571E-6}
 {:time 1392858000000,
  :count 15,
  :min 2.1E-6,
  :max 2.13E-6,
  :total 3.1750000000000006E-5,
  :total_count 15,
  :mean 2.116666666666667E-6}
 {:time 1392858300000,
  :count 18,
  :min 2.12E-6,
  :max 2.13E-6,
  :total 3.830000000000001E-5,
  :total_count 18,
  :mean 2.127777777777778E-6}
 {:time 1392858600000,
  :count 17,
  :min 2.12E-6,
  :max 2.13E-6,
  :total 3.616000000000001E-5,
  :total_count 17,
  :mean 2.1270588235294127E-6}
 {:time 1392858900000,
  :count 16,
  :min 2.11E-6,
  :max 2.13E-6,
  :total 3.398000000000001E-5,
  :total_count 16,
  :mean 2.1237500000000006E-6}
 {:time 1392859200000,
  :count 10,
  :min 2.11E-6,
  :max 2.13E-6,
  :total 2.1220000000000004E-5,
  :total_count 10,
  :mean 2.122E-6}
 {:time 1392859500000,
  :count 13,
  :min 2.11E-6,
  :max 2.13E-6,
  :total 2.7620000000000003E-5,
  :total_count 13,
  :mean 2.124615384615385E-6}
 {:time 1392859800000,
  :count 8,
  :min 2.12E-6,
  :max 2.13E-6,
  :total 1.6989999999999998E-5,
  :total_count 8,
  :mean 2.1237499999999998E-6})
