(ns kraken.api.core
  (:use ;[kraken.api kraken cryptsy]
   [kraken.model]))

;;; here we provide all that is needed to use either of the supported api's, beginning with
;;; selecting one and initializing it.

(defcomponent :exchanges []
  :init )


;;; So below protocols etc need to move to model so that they are accessible from
;;; kraken.api.cryptsy and kraken.api.kraken?
(defprotocol ExchangeP
  (tick-channel [this])
  (trade-channel [this]))

(defrecord Exchange [name instance]
  ComponentP
  (initialize [this system] ((partial initialize ))))

;;;
;; (defcomponent :exchanges
;;   (reify ComponentP
;;     ()))

;; (defn initialize [cfg]
;;   (cond (= :cryptsy (get-in cfg :api )) )
;;   (assoc-in cfg)
;;   )


