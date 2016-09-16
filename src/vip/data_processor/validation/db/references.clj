(ns vip.data-processor.validation.db.references
  (:require [vip.data-processor.validation.db.util :as util]
            [korma.core :as korma]
            [vip.data-processor.errors :as errors]))

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
                          (errors/add-errors ctx :errors table (:id unmatched-reference) :unmatched-reference
                                             {(:name column) (get unmatched-reference (keyword (:name column)))}))
                        ctx unmatched-references)))
            ctx
            reference-columns)))
