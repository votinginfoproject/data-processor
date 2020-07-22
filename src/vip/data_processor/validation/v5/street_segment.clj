(ns vip.data-processor.validation.v5.street-segment
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]
            [clojure.string :as str]
            [vip.data-processor.errors :as errors]
            [clojure.tools.logging :as log]))

(util/validate-no-missing-values :street-segment
                                 [:odd-even-both]
                                 [:city]
                                 [:state]
                                 [:street-name]
                                 [:zip]
                                 [:precinct-id])

(def validate-odd-even-both-value
  (util/validate-enum-elements :oeb-enum :errors))

(defn valid-house-number-or-nil?
  [house-number]
  (or (str/blank? house-number)
      (try
        (Integer/parseInt house-number)
        true
        (catch NumberFormatException ex
          false))))

(def validate-start-house-number
  (util/validate-elements :street-segment
                          [:start-house-number]
                          valid-house-number-or-nil?
                          :fatal
                          :start-house-number))

(def validate-end-house-number
  (util/validate-elements :street-segment
                          [:end-house-number]
                          valid-house-number-or-nil?
                          :fatal
                          :end-house-number))

(defn valid-house-number-fix-or-nil?
  "Validate the prefix or suffix house number. They are restricted to
   alphabetical characters, numbers, and the '/' character."
  [house-number-fix]
  (or (str/blank? house-number-fix)
      (not (str/blank? (re-find #"^[a-zA-z0-9\/-]+$" house-number-fix)))))

;; The next two validations are temporarily being disabled until we hear
;; back from Google exactly what constraints they may want us to check
;; against.
(def validate-house-number-prefix-value
  (util/validate-elements :street-segment
                          [:house-number-prefix]
                          valid-house-number-fix-or-nil?
                          :errors
                          :house-number-prefix))

(def validate-house-number-suffix-value
  (util/validate-elements :street-segment
                          [:house-number-suffix]
                          valid-house-number-fix-or-nil?
                          :errors
                          :house-number-suffix))

(def validate-no-includes-all-addresses-with-house-number-prefix
  (util/build-xml-tree-value-query-validator
   :errors :street-segment :invalid :invalid-house-number-prefix-with-includes-all-addresses
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 5) AS path,
          parent_with_id, simple_path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.HouseNumberPrefix') xtv
    JOIN (SELECT path, parent_with_id
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.IncludesAllAddresses'
          AND value = 'true') xtv2
    ON xtv.parent_with_id = xtv2.parent_with_id"
   util/two-import-ids))

(def validate-no-includes-all-streets-with-house-number-prefix
  (util/build-xml-tree-value-query-validator
   :errors :street-segment :invalid :invalid-house-number-prefix-with-includes-all-streets
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 5) AS path,
          parent_with_id, simple_path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.HouseNumberPrefix') xtv
    JOIN (SELECT path, parent_with_id
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.IncludesAllStreets'
          AND value = 'true') xtv2
    ON xtv.parent_with_id = xtv2.parent_with_id"
   util/two-import-ids))

(def validate-no-includes-all-addresses-with-house-number-suffix
  (util/build-xml-tree-value-query-validator
   :errors :street-segment :invalid :invalid-house-number-suffix-with-includes-all-addresses
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 5) AS path,
          parent_with_id, simple_path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.HouseNumberSuffix') xtv
    JOIN (SELECT path, parent_with_id
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.IncludesAllAddresses'
          AND value = 'true') xtv2
    ON xtv.parent_with_id = xtv2.parent_with_id"
   util/two-import-ids))

(def validate-no-includes-all-streets-with-house-number-suffix
  (util/build-xml-tree-value-query-validator
   :errors :street-segment :invalid :invalid-house-number-suffix-with-includes-all-streets
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 5) AS path,
          parent_with_id, simple_path
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.HouseNumberSuffix') xtv
    JOIN (SELECT path, parent_with_id
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.IncludesAllStreets'
          AND value = 'true') xtv2
    ON xtv.parent_with_id = xtv2.parent_with_id"
   util/two-import-ids))

(def validate-start-end-house-number-with-house-number-prefix
  (util/build-xml-tree-value-query-validator
   :errors :street-segment :invalid :start-and-end-house-numbers-must-be-identical-when-house-number-prefix-specified
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 5) AS path,
          parent_with_id
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.HouseNumberPrefix') xtv
    INNER JOIN (SELECT parent_with_id, value
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.StartHouseNumber') xtv2
    ON xtv.parent_with_id = xtv2.parent_with_id
    INNER JOIN (SELECT parent_with_id, value
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.EndHouseNumber') xtv3
    ON xtv.parent_with_id = xtv3.parent_with_id
    WHERE xtv2.value <> xtv3.value"
   (fn [{:keys [import-id]}]
     [import-id import-id import-id])))

(def validate-start-end-house-number-with-house-number-suffix
  (util/build-xml-tree-value-query-validator
   :errors :street-segment :invalid :start-and-end-house-numbers-must-be-identical-when-house-number-suffix-specified
   "SELECT xtv.path
    FROM (SELECT DISTINCT subltree(path, 0, 5) AS path,
          parent_with_id
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.HouseNumberSuffix') xtv
    INNER JOIN (SELECT parent_with_id, value
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.StartHouseNumber') xtv2
    ON xtv.parent_with_id = xtv2.parent_with_id
    INNER JOIN (SELECT parent_with_id, value
          FROM xml_tree_values WHERE results_id = ?
          AND subltree(simple_path,0,3) = 'VipObject.StreetSegment.EndHouseNumber') xtv3
    ON xtv.parent_with_id = xtv3.parent_with_id
    WHERE xtv2.value <> xtv3.value"
   (fn [{:keys [import-id]}]
     [import-id import-id import-id])))

(defn validate-no-street-segment-overlaps
  [{:keys [import-id] :as ctx}]
  (log/info "Validating street segment overlaps")
  (let [overlaps (korma/exec-raw
                  (:conn postgres/v5-2-street-segments)
                  ["SELECT * from street_segment_overlaps(?);" [import-id]]
                  :results)]
    (reduce (fn [ctx overlap]
              (let [path (-> :path overlap .getValue)
                    parent-element-id (util/get-parent-element-id path import-id)]
                (errors/add-v5-errors ctx
                                   :errors
                                   :street-segment
                                   path
                                   :overlaps
                                   parent-element-id
                                   (:id overlap))))
            ctx overlaps)))
