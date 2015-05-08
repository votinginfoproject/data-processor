(ns vip.data-processor.validation.db.references
  (:require [vip.data-processor.validation.db.util :as util]
            [korma.core :as korma]))

(defn unmatched-references [tables from via to]
  (let [from-table (tables from)
        to-table (tables to)
        from-table-name (:name from-table)
        to-table-name (:name to-table)
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
                (if (seq unmatched-references)
                  (assoc-in ctx [:errors
                                 table
                                 :reference-error]
                            {(:name column) unmatched-references})
                  ctx)))
            ctx
            reference-columns)))

(defn unmatched-jurisdiction-references [tables from-table]
  (let [table (tables from-table)
        table-name (:name table)
        jurisdiction-id (util/column-name table-name "jurisdiction_id")]
    (korma/select table
                  (korma/join :left (:states tables) (= :states.id jurisdiction-id))
                  (korma/join :left (:localities tables) (= :localities.id jurisdiction-id))
                  (korma/join :left (:precincts tables) (= :precincts.id jurisdiction-id))
                  (korma/join :left (:precinct-splits tables) (= :precinct_splits.id jurisdiction-id))
                  (korma/join :left (:electoral-districts tables) (= :electoral_districts.id jurisdiction-id))
                  (korma/where (and (not= jurisdiction-id "")
                                    (= :states.id nil)
                                    (= :localities.id nil)
                                    (= :precincts.id nil)
                                    (= :precinct_splits.id nil)
                                    (= :electoral_districts.id nil))))))

(defn validate-jurisdiction-reference [ctx {:keys [filename table]}]
  (let [unmatched-references (unmatched-jurisdiction-references
                               (:tables ctx) table)]
    (if (seq unmatched-references)
      (assoc-in ctx [:errors table :reference-error]
                {:jurisdiction-id unmatched-references})
      ctx)))
