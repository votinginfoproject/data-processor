(ns vip.data-processor.validation.v5.locality
  (:require [vip.data-processor.validation.v5.util :as util]
            [vip.data-processor.validation.xml.spec :as spec]
            [clojure.string :as str]))

(def locality-paths
  (spec/type->simple-paths "Locality" "5.0"))

(defn clojure-type->xml-type [type]
  (let [components (-> type name (str/split #"-"))]
    (str/join (map str/capitalize components))))

(defn build-validators [type import-id]
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
  (let [validators (build-validators :name import-id)]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))

(defn validate-no-missing-state-ids [{:keys [import-id] :as ctx}]
  (let [validators (build-validators :state-id import-id)]
    (reduce (fn [ctx validator] (validator ctx)) ctx validators)))
