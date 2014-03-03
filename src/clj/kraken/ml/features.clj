(ns kraken.ml.features
  (:use [kraken.model]
        [kraken.system]
        [kraken.view.charts]
        )
  (:require [incanter.core :as inc]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat]))

;; TODO: 
;; - use weighted averages to calculate price curve => use "total" field returned by cryptsy
;; - think about higher order differentials
;; - features from spread at different percentages of current price

;;; 1) For different average intervals T and k:
;;;
;;;    (price(now) - price(now-k*T))/price(now)

(defn differentiate-seq 
  ([s] (differentiate-seq s 0.5))
  ([s alpha]
     (let [beta (- 1.0 alpha)]
       (map #(/ (- %2 %1) (+ (* alpha %1) (* beta %2))) 
            s (rest s)))))

(defn- format-interval [I]
  (str (tcore/in-seconds I) "s"))

(defn relative-price-changes [system t T K]
  (let [t-KT (reduce #(tcore/minus %1 %2) 
                     t
                     (repeat K T))
        pc (price-curve system 
                        (format-interval (tcore/interval (tcore/minus t T) t))
                        :filter {:range {:time {:from t-KT :to t}}})
        prices (map :mean pc)]
    (map #(* 100.0 %)
         (differentiate-seq prices))))

(defn relative-logprice-changes [system t T K]
  (map #(* % (Math/log (Math/abs %))) 
       (relative-price-changes system t T K)))

;;; NOW we need to determine the training label of a set of features. Ideally, it consists of a price (change) distribution over future price changes.

(defn avg [c]
  (when-not (empty? c)
    (/ (reduce + c) (count c))))

(defn dyn-avg [c alpha]
  (when-not (empty? c)
    (let [beta (- 1.0 alpha)]
      (reduce #(+ (* beta %1) (* alpha %2)) c))))
  
(defn vw-label [t T]
  ;; (price(t+T)-price(t))/price(t)
  (let [cur-price (dyn-avg (map :price (trades (system) 
                                               :filter {:range {:time {:from (tcore/minus t T) :to t}}}
                                               :size 1000))
                           0.6)
        fut-price (dyn-avg (map :price (trades (system) 
                                               :filter {:range {:time {:from t :to (tcore/plus t T)}}}
                                               :size 1000))
                           0.6)]
    (when (and cur-price fut-price)
      (/ (- cur-price fut-price) cur-price))))

(defn- vw-format [features]
  (mapcat (fn [[T prices]]
            (map #(str "T" T "_" %1 ":" %2)
                 (range)
                 prices))
          features))

(defn vw-features [t T]
  (clojure.string/join 
           " "
           (vw-format (for [Tf (concat (map #(tcore/minutes %) [1 2 5 10 20])
                                       (map #(tcore/hours %) [1 2 6 12])
                                       (map #(tcore/days %) [1 2 5])
                                       (map #(tcore/weeks %) [1 2 3])
                                       (map #(tcore/months %) [1 2 3 4 5 6 7 8 9 10 11]))]
                        [(str Tf) (relative-price-changes (system) t Tf 10)]))))
(defn vw-datum [t T]
  (str (vw-label t T)
       " | "
       (clojure.string/join 
        " "
        (vw-features t T))))

;; (def t kraken.ml.vw/gt)
;; (vw-format (for [Tf (concat (map #(tcore/minutes %) [1 2 5 10 20])
;;                             (map #(tcore/hours %) [1 2 6 12])
;;                             (map #(tcore/days %) [1 2 5])
;;                             (map #(tcore/weeks %) [1 2 3])
;;                             (map #(tcore/months %) [1 2 3 4 5 6 7 8 9 10 11]))]
;;              [(str Tf) (relative-price-changes (system) t Tf 10)]))
;; (compute-label t T)
;; (def t (tcore/now))
;; (def T (tcore/minutes 5))     
;; (vw-features t T true)
;; (vw-features t T)


;; think from the perspective of the moment. At each moment, we can buy, sell, or do
;; nothing. Whatever the decision, a new one needs to be made again every next time step.

;; we want to predict direction and size of change before the next time we can make a decision

;; => labels -100, -80, .., 0, .. , 80, 100

;; => after learning we predict class probabilities and calculate the distribution DOGE/BTC that is most likely to generate the most benifit, e.g. the total value in DOGE

;; Only remaining problem is what time period to use for the predicion. Or is it? Maybe depends on
;; the way decisions are made?

;; Assume that our current balance is [d,b], and the current price is p(k) (i.e. 1 d is p b). The total amount of d, D, is thus d+b/p. If the price at time k+1 is p(k+1), then the total amount of dodge we have after selling s dodge, 0<=s<=d, is  (d*p(k)+b)/p(k+1):

;; 1) sell s at price p(k) => (d-s) + s*p(k)+b
;; 2) buy d at price p(k+1) => (d-s) + (s*p(k)+b)/p(k+1)


;; Thus, relative to the current D this is ...

;; Conclusion: if p(k+1)<p(k+1) => sell, ...

;; Now rephrase in terms of probabilities and expeced payoff

;; P(k+1) => expected D(k+1) when selling s = ...




;;; 2) spread at different percentages of current price

;;; => need spread channel
