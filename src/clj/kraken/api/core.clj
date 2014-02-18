(ns kraken.api.core
  (:use [kraken.model]
        [kraken.api.cryptsy])
  (:require [clojure.core.async :as as]))

(full-reset!)

(system)
;;; external configuration of cryptsy:
(configure! [:exchanges :cryptsy] (read-string (slurp (str (System/getProperty "user.home") "/.cryptsy/config.edn"))))

(initialize!)
(shutdown!)
(start!)
(stop!)

;; TODO: 
;; - make cryptsy component factory ("cryptsy-component")
;;   => make each component a new record implementing component protocol
;; - make fresh components in full-reset

;;; TODO:
;;; - also make component for kraken
;;; - then see what next, maybe make exchanges component, 
;;; - make elastic component (should go in kraken.elastic...)
;;; - combine (in kraken.core)


