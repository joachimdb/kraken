(ns kraken.api.core
  (:use [kraken.model]
        [kraken.api.cryptsy]))

;;; here we provide all that is needed to use either of the supported api's, beginning with
;;; selecting one and initializing it.

;;; initial configuration of cryptsy:
(swap! +system+ #(initialize-component
                  (configure % [:exchanges :cryptsy] (read-string (slurp (str (System/getProperty "user.home") "/.cryptsy/config.edn"))))
                  [:exchanges :cryptsy]))

;;; So below protocols etc need to move to model so that they are accessible from
;;; kraken.api.cryptsy and kraken.api.kraken?
;; (defprotocol ExchangeP
;;   (tick-channel [this])
;;   (trade-channel [this]))

;; (defrecord Exchange [name instance]
;;   ComponentP
;;   (initialize [this system] ((partial initialize ))))

;;;
;; (defcomponent :exchanges
;;   (reify ComponentP
;;     ()))

;; (defn initialize [cfg]
;;   (cond (= :cryptsy (get-in cfg :api )) )
;;   (assoc-in cfg)
;;   )


