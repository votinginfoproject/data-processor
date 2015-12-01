(ns vip.data-processor.validation.db.references
  (:require [vip.data-processor.validation.db.util :as util]
            [korma.core :as korma]))

(defn unmatched-references [tables from via to]
  (let [from-table (tables from)
        to-table (tables to)
        from-table-name (:alias from-table)
        to-table-name (:alias to-table)
        via-join-name (util/column-name from-table-name via)
        to-id-name (util/column-name to-table-name "id")]
    (korma/select from-table
                  (korma/join :left to-table
                              (= via-join-name to-id-name))
                  (korma/where
                   (and (= to-id-name nil)
                        (not= via-join-name ""))))))

(defn validate-references-for-data-spec [ctx {:keys [filename table columns]}]
  (let [reference-columns (filter :references columns)]
    (reduce (fn [ctx column]
              (let [unmatched-references (unmatched-references
                                          (:tables ctx)
                                          table
                                          (:name column)
                                          (:references column))]
                (reduce (fn [ctx unmatched-reference]
                          (update-in ctx [:errors table (:id unmatched-reference) :unmatched-reference]
                                     conj {(:name column) (get unmatched-reference (keyword (:name column)))}))
                        ctx unmatched-references)))
            ctx
            reference-columns)))

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
              (update-in ctx [:errors table (:id unmatched-reference) :unmatched-reference]
                         conj (select-keys unmatched-reference [:jurisdiction_id])))
            ctx unmatched-references)))
