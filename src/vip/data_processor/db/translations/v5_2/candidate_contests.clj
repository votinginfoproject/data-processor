(ns vip.data-processor.db.translations.v5-2.candidate-contests
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(defn candidate-contests
  [import-id]
  (korma/select (postgres/v5-2-tables :candidate-contests)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.CandidateContest." index))

(defn candidate-contest->ltree-entries
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
              (util/simple-value->ltree :other_vote_variation)
              (util/simple-value->ltree :number_elected)
              (util/simple-value->ltree :office_ids)
              (util/simple-value->ltree :primary_party_ids)
              (util/simple-value->ltree :votes_allowed)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id row)})))

(def transformer
  (util/transformer
   candidate-contests
   candidate-contest->ltree-entries))
