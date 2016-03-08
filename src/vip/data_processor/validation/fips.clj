(ns vip.data-processor.validation.fips
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(def fips-columns
  [:state :state-fips :county-fips :county-name :class-code])

(defn full-county-fips [{:keys [state-fips county-fips]}]
  (str state-fips county-fips))

(def fips
  (->> "fips.txt"
       io/resource
       slurp
       csv/read-csv
       (map (partial zipmap fips-columns))
       (map (juxt :state-fips full-county-fips))
       flatten
       set))

(defn valid-fips? [fips-code]
  (fips fips-code))
