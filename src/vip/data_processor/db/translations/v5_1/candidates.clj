(ns vip.data-processor.db.translations.v5-1.candidates
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]))

(defn candidates [import-id]
  (korma/select (postgres/v5-1-tables :candidates)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.Candidate." index))

(defn candidate->ltree-entries [idx-fn candidate]
  (let [candidate-path (base-path (idx-fn))
        id-path (util/id-path candidate-path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn candidate-path candidate)
             [(util/internationalized-text->ltree :ballot_name)
              util/external-identifiers->ltree
              (util/simple-value->ltree :file_date)
              (util/simple-value->ltree :is_incumbent)
              (util/simple-value->ltree :is_top_ticket)
              (util/simple-value->ltree :party_id)
              (util/simple-value->ltree :person_id)
              (util/simple-value->ltree :post_election_status)
              (util/simple-value->ltree :pre_election_status)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id candidate)})))

(def transformer (util/transformer candidates
                                   candidate->ltree-entries))
