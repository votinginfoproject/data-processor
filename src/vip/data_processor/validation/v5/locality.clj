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

(defn validate-no-missing-names [{:keys [import-id] :as ctx}]
  (let [validators (util/build-no-missing-validators :locality :name import-id)]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))

(defn validate-no-missing-state-ids [{:keys [import-id] :as ctx}]
  (let [validators (util/build-no-missing-validators :locality :state-id
                                                     import-id)]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))

(defn valid-type? [type] (valid-types type))

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
