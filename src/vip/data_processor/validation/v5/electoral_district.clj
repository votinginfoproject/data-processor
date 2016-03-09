(ns vip.data-processor.validation.v5.electoral-district
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-names
  (util/build-xml-tree-value-query-validator
   :errors :electoral-district :missing :missing-name
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'Name' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(path, 0, 4) ~ 'VipObject.0.ElectoralDistrict.*{1}') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def validate-no-missing-types
  (util/build-xml-tree-value-query-validator
   :errors :electoral-district :missing :missing-type
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 4) || 'Type' AS path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(path, 0, 4) ~ 'VipObject.0.ElectoralDistrict.*{1}') xtv
    LEFT JOIN (SELECT path FROM xml_tree_values WHERE results_id = ?) xtv2
    ON xtv.path = subltree(xtv2.path, 0, 5)
    WHERE xtv2.path IS NULL"
   util/two-import-ids))

(def types
  #{"city" "city-council" "congressional" "county" "county-council" "judicial"
    "municipality" "national" "school" "special" "state" "state-house"
    "state-senate" "town" "township" "utility" "village" "ward" "water"
    "other"})

(defn valid-type? [type] (types type))

(defn validate-type-formats [{:keys [import-id] :as ctx}]
  (let [imported-types (util/select-lquery
                        import-id
                        "VipObject.0.ElectoralDistrict.*{1}.Type.*{1}")
        invalid-types (remove (comp valid-type? :value) imported-types)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :electoral-district (-> row :path .getValue)
                          :format] conj (:value row)))
            ctx invalid-types)))
