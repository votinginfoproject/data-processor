(ns vip.data-processor.validation.data-spec.translate
  (:require [clojure.string :as str])
  (:import [java.text Normalizer]))

(defn clean-text [v]
  (str/replace v #"\R" "\n"))
