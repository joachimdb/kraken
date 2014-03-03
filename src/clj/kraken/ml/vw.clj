(ns kraken.ml.vw
  (:use [kraken.model]
        [kraken.system]
        [kraken.elastic]
        [kraken.ml.features])
  (:require [clj-time.core :as tcore]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [incanter.core :as inc]
            [incanter.charts :as ich]))

(defn vw-connection [host port]
  (let [socket (java.net.Socket. host port)
        in (java.io.BufferedReader. (java.io.InputStreamReader. (.getInputStream socket)))
        out (java.io.PrintWriter. (.getOutputStream socket) true)]
    {:in in :out out :socket socket}))

(defmacro with-vw-connection [[con & {:keys [host port]
                                      :or {host "localhost"
                                           port 26542}}] 
                              & body] 
  (let [ret (gensym)]
    `(let [~con (vw-connection ~host ~port)
           ~ret (do ~@body)]
       (.close (:in ~con))
       (.close (:out ~con))
       (.close (:socket ~con))
       ~ret)))

(defn train [vw-connection datum]
  (.println (:out vw-connection) datum)
  (.readLine (:in vw-connection)))


(defn predict [vw-connection datum]
  (.println (:out vw-connection) datum)
  (.readLine (:in vw-connection)))


(def T (tcore/minutes 10))
(def start-time (tcore/plus (:time (first (trades (system) :sort {:time {:order "asc"}}
                                                  :size 1)))
                            T))
(def end-time (tcore/ago (tcore/hours 3)))

(def time-points
  (loop [times [start-time]]
    (if (tcore/before? (last times) (tcore/now))
      (recur (conj times (tcore/plus (last times) T)))
      times)))

(defn prediction [t T]
  (with-vw-connection [c]
    (predict c (vw-datum t T false))))


(count time-points) ;; 252

(count (filter #(tcore/after? end-time %) time-points)) ;; 239


(def labels (map #(vw-label % T) time-points))

(def features (map #(vw-features % T) time-points))

(let [n (count (filter #(tcore/before? % end-time) time-points))]
  (with-vw-connection [c]
    (doseq [i (range (count (filter #(tcore/before? % end-time) time-points))) ]
      (println (str i "/" n))
      (train c (str (nth labels i) " | "(nth features i))))))


(def predictions
  (with-vw-connection [c]
    (doall (for [i (range (count time-points))]
             (do (println (str i "/" (count time-points)))
                 (predict c (str  " | " (nth features i))))))))

(let [times (filter #(tcore/before? % end-time) time-points)
      n-train (count times)
      ch (ich/scatter-plot (take n-train labels) 
                           (map read-string (take n-train predictions))
                           :x-label "Actual"
                           :y-label "Prediction"
                           :legend true
                           :series-label "Training samples")]
  (ich/add-points ch (drop n-train labels) 
                  (map read-string (drop n-train predictions))
                  :series-label "Predictions")
  (inc/view ch))

(let [n-train (count (filter #(tcore/before? % end-time) time-points))
      time-points (map tcoerce/to-long time-points)
      ch (ich/xy-plot time-points
                           labels
                           :x-label "Time"
                           :y-label "Actual"
                           :legend true
                           :series-label "Training samples")]
  (ich/add-lines ch (take n-train time-points) 
                  (map read-string (take n-train predictions))
                  :series-label "Trained predictions")
  (ich/add-lines ch (drop n-train time-points) 
                  (map read-string (drop n-train predictions))
                  :series-label "Predictions")
  (inc/view ch))



;; ;;; Tutorial house Example from https://github.com/JohnLangford/vowpal_wabbit/wiki/Tutorial (assumes that vw is running in daemon mode at localhost:26542)

;; ;;; Online:

0 | price:.23 sqft:.25 age:.05 2006
1 2 'second_house | price:.18 sqft:.15 age:.35 1976
0 1 0.5 'third_house | price:.53 sqft:.32 age:.87 1924

;; (def d1 (datum 0 #{(feature-set #{(feature "price" 0.23) (feature "sqft" 0.25) (feature "age" 0.05) (feature 2006)})}))
;; (def d2 (datum 1 2 nil 'second_house #{(feature-set #{(feature "price" 0.18) (feature "sqft" 0.15) (feature "age" 0.35) (feature 1976)})}))
;; (def d3 (datum 0 1 0.5 'third_house #{(feature-set #{(feature "price" 0.53) (feature "sqft" 0.32) (feature "age" 0.87) (feature 1924 nil)})}))
;; (vwformat d1)
;; ;; => "0 | sqft:0.25 price:0.23 age:0.05 2006\n"
;; (vwformat d2)
;; ;; => "1 2 'second_house | sqft:0.15 1976 price:0.18 age:0.35\n"
;; (vwformat d3)
;; ;; => "0 1 0.5 'third_house | sqft:0.32 1924 price:0.53 age:0.87\n"

;; (with-vw-connection [c]
;;   (train c d1))
;; ;; => "0.000000"
;; (with-vw-connection [c]
;;   (train c d2))
;; ;;; => "0.000000 second_house"
;; (with-vw-connection [c]
;;   (train c d3))
;; ;;; => "1.000000 third_house"

;; ;;; Or generate train file:
;; (doseq [d [d1 d2 d3]]
;;   (spit "/tmp/house_data" (vwformat d) :append true))
