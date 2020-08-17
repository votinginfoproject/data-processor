(ns vip.data-processor.validation.v5.id
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [vip.data-processor.validation.xml.spec :as spec]
            [vip.data-processor.errors :as errors]
            [clojure.tools.logging :as log]))

(defn duplicate-ids [import-id]
  (korma/exec-raw
   (:conn postgres/xml-tree-values)
   ["with ids as (select value, path
                  from xml_tree_values
                  where results_id = ?
                    and path ~ 'VipObject.0.*.id'
                  order by value)
     select set1.path, set1.value
     from ids set1
     inner join ids set2
             on set1.value = set2.value
            and set1.path <> set2.path"
    [import-id]]
   :results))

(defn validate-unique-ids
  [{:keys [import-id] :as ctx}]
  (log/info "Validating unique ids")
  (let [duplicate-ids (duplicate-ids import-id)]
    (reduce (fn [ctx row]
              (let [id (:value row)
                    path (-> row :path .getValue)]
                (errors/add-errors ctx :fatal :id path :duplicates id)))
            ctx duplicate-ids)))

(def validate-no-missing-ids
  (util/build-xml-tree-value-query-validator
   :fatal :id :missing :missing-id
   "select distinct subltree(path, 0, 4) || 'id' as path
    from xml_tree_values
    where results_id = ?
      and nlevel(path) > 3
      and parent_with_id is null"
   (fn [{:keys [import-id]}] [import-id])))

(defn validate-ids-with
  [{:keys [import-id] :as ctx} simple-path query]
  (let [bad-ids (korma/exec-raw
                 (:conn postgres/xml-tree-values)
                 query
                 :results)]
    (reduce (fn [ctx bad-id]
              (errors/add-errors ctx
                                 :errors :id (.getValue (:path bad-id))
                                 :no-referent
                                 (str (:value bad-id))))
            ctx bad-ids)))

(defn validate-idref-type-refers
  [{:keys [import-id] :as ctx} simple-path]
  (validate-ids-with ctx simple-path
   ["SELECT xtv.path, xtv.value FROM xml_tree_values xtv
     LEFT JOIN (SELECT value FROM xml_tree_values
                 WHERE results_id = ? AND simple_path ~ '*{2}.id') xtv2
            ON xtv.value = xtv2.value
         WHERE xtv.results_id = ?
           AND xtv.simple_path = ?
           AND xtv2.value IS NULL"
    [import-id import-id simple-path]]))

(defn validate-idrefs-type-refers
  "Look at `xs:IDREFS` types and find those that do not have a referent"
  [{:keys [import-id] :as ctx} simple-path]
  (validate-ids-with ctx simple-path
   ["WITH referer
       AS (SELECT results_id, path,
                  UNNEST(string_to_array(value, ' ')) AS value,
                  parent_with_id, simple_path
             FROM xml_tree_values
            WHERE simple_path = ?
              and results_id = ?)
     SELECT referer.path, referer.value
       FROM referer
       LEFT JOIN (SELECT value
                    FROM xml_tree_values
                   WHERE results_id = ?
                     AND simple_path ~ '*{2}.id') referent
              ON referer.value = referent.value
           WHERE referent.value IS NULL;"
    [simple-path import-id import-id]]))

(defn validate-idref-references
  [{:keys [import-id spec-version] :as ctx}]
  (log/info "Validating idref references")
  (let [idref-simple-paths (->> (spec/type->simple-paths "xs:IDREF" spec-version)
                                (map postgres/path->ltree))]
    (reduce validate-idref-type-refers
            ctx idref-simple-paths)))

(defn validate-idrefs-references
  [{:keys [import-id spec-version] :as ctx}]
  (log/info "Validating idrefs references")
  (let [idrefs-simple-paths (->> (spec/type->simple-paths "xs:IDREFS" spec-version)
                                 (map postgres/path->ltree))]
    (reduce validate-idrefs-type-refers
            ctx idrefs-simple-paths)))
