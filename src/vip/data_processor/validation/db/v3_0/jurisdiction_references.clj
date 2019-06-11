(ns vip.data-processor.validation.db.v3-0.jurisdiction-references
  (:require [vip.data-processor.validation.db.util :as util]
            [korma.core :as korma]
            [vip.data-processor.errors :as errors]))

(defn unmatched-jurisdiction-references [tables from-table]
  (let [table (tables from-table)
        table-name (:alias table)
        jurisdiction-id (util/column-name table-name "jurisdiction_id")]
    (korma/select table
                  (korma/join :left
                              [(:states tables) :states]
                              (= :states.id jurisdiction-id))
                  (korma/join :left
                              [(:localities tables) :localities]
                              (= :localities.id jurisdiction-id))
                  (korma/join :left
                              [(:precincts tables) :precincts]
                              (= :precincts.id jurisdiction-id))
                  (korma/join :left
                              [(:precinct-splits tables) :precinct_splits]
                              (= :precinct_splits.id jurisdiction-id))
                  (korma/join :left
                              [(:electoral-districts tables) :electoral_districts]
                              (= :electoral_districts.id jurisdiction-id))
                  (korma/where (and (not= jurisdiction-id "")
                                    (= :states.id nil)
                                    (= :localities.id nil)
                                    (= :precincts.id nil)
                                    (= :precinct_splits.id nil)
                                    (= :electoral_districts.id nil))))))

(defn validate-jurisdiction-reference [ctx {:keys [filename table]}]
  (let [unmatched-references (unmatched-jurisdiction-references
                              (:tables ctx) table)]
    (reduce (fn [ctx unmatched-reference]
              (errors/add-errors ctx :errors table (:id unmatched-reference) :unmatched-reference
                                 (select-keys unmatched-reference [:jurisdiction_id])))
            ctx unmatched-references)))

(defn validate-jurisdiction-references [{:keys [data-specs] :as ctx}]
  (let [jurisdiction-tables (filter
                               (fn [spec] (some #{"jurisdiction_id"}
                                                (map :name (:columns spec))))
                               data-specs)]
    (reduce validate-jurisdiction-reference ctx jurisdiction-tables)))
