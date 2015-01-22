(ns vip.data-processor.validation.transforms
  (:require [clojure.edn :as edn]
            [vip.data-processor.s3 :as s3]))

(defn read-edn-sqs-message [ctx]
  (assoc ctx :input (edn/read-string (get-in ctx [:input :body]))))

(defn assert-filename [ctx]
  (if-let [filename (get-in ctx [:input :filename])]
    ctx
    (assoc ctx :stop "No filename!")))

(defn download-from-s3 [ctx]
  (let [filename (get-in ctx [:input :filename])
        file (s3/download filename)]
    (assoc ctx :input file)))
