(ns vip.data-processor.validation.fips
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
        vip-id (-> (korma/select sources (korma/fields :vip_id))
                   first
                   :vip_id)]
    (if (valid-fips? vip-id)
      ctx
      (assoc-in ctx [:errors :source :invalid-vip-id] vip-id))))
