(ns vip.data-processor.validation.v5.locality
  (:require [vip.data-processor.validation.v5.util :as util]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.xml.spec :as spec]))

(def valid-types
  #{"city" "city-council" "congressional" "county" "county-concil" "judicial"
    "municipality" "national" "school" "special" "state" "state-house"
    "state-senate" "town" "township" "utility" "village" "ward" "water"
    "other"})

(def validate-no-missing-names
  (util/validate-no-missing-elements :locality [:name]))

(def validate-no-missing-state-ids

(defn valid-type? [type] (valid-types type))
  (util/validate-no-missing-elements :locality [:state-id]))

(defn validate-types [ctx]
  (let [validators (for [p (spec/type->simple-paths "Locality" "5.0")]
                     (fn [{:keys [import-id] :as ctx}]
                       (let [type-path (str p ".Type")
                             types (korma/select postgres/xml-tree-values
                                     (korma/where
                                      {:results_id import-id
                                       :simple_path (postgres/path->ltree
                                                     type-path)}))
                             invalid-types (remove (comp valid-type? :value)
                                                   types)]
                         (reduce (fn [ctx row]
                                   (update-in
                                    ctx
                                    [:errors :locality (-> row :path .getValue)
                                     :format]
                                    conj (:value row)))
                                 ctx invalid-types))))]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))
