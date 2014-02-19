(ns kraken.ml.vw
  (:use [kraken.model]
        [kraken.system]
        [kraken.elastic])
  (:require [clj-time.core :as tcore]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]))

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

(defprotocol VWFormatP
  (vwformat [this]))

;;; VW features: see https://github.com/JohnLangford/vowpal_wabbit/wiki/Input-format. General format:
;;; [Label] [Importance [Tag]]|Namespace Features |Namespace Features ... |Namespace Features

;;; a feature has a name and optionally a value
(defrecord Feature [name value]
  VWFormatP
  (vwformat [this] (str name (when value (str ":" value)))))
(defn feature 
  ([name] (feature name nil))
  ([name value] (Feature. name value)))
;; (defn map->feature [m]
;;   (feature (:name m) (:value m)))

;;; a feature set has a namesapce and a set of features
(defrecord FeatureSet [namespace features]
  VWFormatP 
  (vwformat [this] (str (apply str (or namespace " ") (interpose " " (doall (map vwformat features)))) "\n")))
(defn feature-set
  ([features] (feature-set nil features))
  ([namespace features] (FeatureSet. namespace features)))
;; (defn map->feature-set [m]
;;   (feature-set (:namespace m)
;;                (map map->feature (:features m))))
;;; A vw datum has a label, optionally an importance and a tag, and a set of named feature sets
(defrecord Datum [label importance initial-prediction tag feature-sets]
  VWFormatP
  (vwformat [this] (apply str label
                          " "
                          (when importance 
                            (str importance 
                                 (when initial-prediction (str " " initial-prediction))
                                 (when tag (str " '" tag))
                                 " "))
                          (interleave (repeat "|")
                                      (map vwformat feature-sets)))))
(defn datum
  ([feature-sets] (datum nil nil nil nil feature-sets))
  ([label feature-sets] (datum label nil nil nil feature-sets))  
  ([label importance feature-sets] (datum label importance nil nil feature-sets))  
  ([label importance initial-prediction tag feature-sets]
     (Datum. label importance initial-prediction tag feature-sets)))
;; (defn map->datum [m]
;;   (datum (:label m)
;;          (:importance m)
;;          (:initial-prediction m)
;;          (:tag m)
;;          (map map->feature-set (:feature-sets m))))

(defn train [vw-connection datum]
  (.println (:out vw-connection) (vwformat datum))
  (.readLine (:in vw-connection)))

(defn predict [vw-connection datum]
  (.println (:out vw-connection) (vwformat (assoc datum :label nil)))
  (.readLine (:in vw-connection)))

;; ;;; Tutorial house Example from https://github.com/JohnLangford/vowpal_wabbit/wiki/Tutorial (assumes that vw is running in demon mode at localhost:26542)

;; ;;; Online:

0 | price:.23 sqft:.25 age:.05 2006
1 2 'second_house | price:.18 sqft:.15 age:.35 1976
0 1 0.5 'third_house | price:.53 sqft:.32 age:.87 1924

;; (def d1 (datum 0 #{(feature-set #{(feature "price" 0.23) (feature "sqft" 0.25) (feature "age" 0.05) (feature 2006)})}))
;; (def d2 (datum 1 2 nil 'second_house #{(feature-set #{(feature "price" 0.18) (feature "sqft" 0.15) (feature "age" 0.35) (feature 1976)})}))
;; (def d3 (datum 0 1 0.5 'third_house #{(feature-set #{(feature "price" 0.53) (feature "sqft" 0.32) (feature "age" 0.87) (feature 1924 nil)})}))
(def f (vwformat d1))
"aaa"
(type f)
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
