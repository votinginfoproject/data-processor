(ns vip.data-processor.validation.db.v3-0.fips
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [korma.core :as korma]))

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

(defn validate-valid-source-vip-id [ctx]
  (let [sources (get-in ctx [:tables :sources])
        source (-> sources (korma/select (korma/fields :id :vip_id)) first)
        {:keys [id vip_id]} source]
    (if (valid-fips? vip_id)
      ctx
      (assoc-in ctx [:errors :sources id :invalid-vip-id] [vip_id]))))
