(ns vip.data-processor.validation.v5.locality
  (:require [vip.data-processor.validation.v5.util :as util]
            [vip.data-processor.validation.xml.spec :as spec]
            [clojure.string :as str]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]))

(def valid-types
  #{"city" "city-council" "congressional" "county" "county-concil" "judicial"
    "municipality" "national" "school" "special" "state" "state-house"
    "state-senate" "town" "township" "utility" "village" "ward" "water"
    "other"})

(def locality-paths
  (spec/type->simple-paths "Locality" "5.0"))

(defn clojure-type->xml-type [type]
  (let [components (-> type name (str/split #"-"))]
    (str/join (map str/capitalize components))))

(defn build-no-missing-validators [type import-id]
  (let [path-element (clojure-type->xml-type type)]
    (for [p locality-paths]
      (util/build-xml-tree-value-query-validator
       :errors :locality :missing (->> type name (str "missing-") keyword)
       "SELECT xtv.path
        FROM (SELECT DISTINCT subltree(path, 0, 4) || ? AS path
              FROM xml_tree_values WHERE results_id = ?
              AND subltree(simple_path, 0, 2) = text2ltree(?)) xtv
        LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
        ON xtv.path = subltree(xtv2.path, 0, 5)
        WHERE xtv2.path IS NULL"
       (constantly [path-element import-id p import-id])))))

(defn validate-no-missing-names [{:keys [import-id] :as ctx}]
  (let [validators (build-no-missing-validators :name import-id)]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))

(defn validate-no-missing-state-ids [{:keys [import-id] :as ctx}]
  (let [validators (build-no-missing-validators :state-id import-id)]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))

(defn valid-type? [type] (valid-types type))

(defn validate-types [ctx]
  (let [validators (for [p locality-paths]
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
