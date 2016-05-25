(ns vip.data-processor.db.translations.v5-1.contests
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(defn contests
  "Returns a seq of all contests rows for `import-id`"
  [import-id]
  (korma/select (postgres/v5-1-tables :contests)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.Contest." index))

(defn contest->ltree-entries
  "The contest we're transforming is the `row`.
  The `idx-fn` is any function that yields monotonically increasing values when
  called - must be thread-safe.

  Returns a seq of maps that each represent a row to insert into `xml_tree_values`."
  [idx-fn row]
  (let [path (base-path (idx-fn))
        id-path (util/id-path path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn path row)
             [(util/simple-value->ltree :abbreviation)
              (util/simple-value->ltree :ballot_selection_ids)
              (util/internationalized-text->ltree :ballot_sub_title)
              (util/internationalized-text->ltree :ballot_title)
              (util/simple-value->ltree :electoral_district_id)
              (util/internationalized-text->ltree :electorate_specification)
              util/external-identifiers->ltree
              (util/simple-value->ltree :has_rotation)
              (util/simple-value->ltree :name)
              (util/simple-value->ltree :sequence_order)
              (util/simple-value->ltree :vote_variation)
              (util/simple-value->ltree :other_vote_variation)])
     {:path id-path
      :simple_path "VipObject.Contest.id"
      :parent_with_id id-path
      :value (:id row)})))

(def transformer
 (util/transformer
  contests
  contest->ltree-entries))
