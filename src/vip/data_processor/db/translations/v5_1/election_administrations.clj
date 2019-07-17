(ns vip.data-processor.db.translations.v5-1.election-administrations
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]
            [vip.data-processor.db.translations.v5-1.departments :as ds]
            [vip.data-processor.db.translations.v5-1.voter-services :as vs]))

(defn row-fn [import-id]
  (korma/select (postgres/v5-1-tables :election-administrations)
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.ElectionAdministration." index))

(defn election-administrations->ltree [import-id idx-fn]
  (let [election-administrations (row-fn import-id)
        departments (group-by :election_administration_id (ds/row-fn import-id))
        voter-services (group-by :department_id (vs/row-fn import-id))]
    (mapcat (fn [ea]
              (let [ds (get departments (:id ea))
                    path (base-path (idx-fn))
                    id-path (util/id-path path)
                    child-idx-fn (util/index-generator 0)]
                (conj
                 (mapcat #(% child-idx-fn path ea)
                         [(util/simple-value->ltree :absentee_uri)
                          (util/simple-value->ltree :am_i_registered_uri)
                          (ds/departments->ltree ds voter-services id-path)
                          (util/simple-value->ltree :elections_uri)
                          (util/simple-value->ltree :registration_uri)
                          (util/simple-value->ltree :rules_uri)
                          (util/simple-value->ltree :what_is_on_my_ballot_uri)
                          (util/simple-value->ltree :where_do_i_vote_uri)])
                 {:path id-path
                  :simple_path (util/path->simple-path id-path)
                  :parent_with_id id-path
                  :value (:id ea)})))
            election-administrations)))

(defn transformer [{:keys [import-id ltree-index] :as ctx}]
  (let [idx-fn (util/index-generator ltree-index)
        rows (util/prep-for-insertion
              import-id
              (election-administrations->ltree import-id idx-fn))]
    (if (seq rows)
      (-> ctx
            (postgres/bulk-import postgres/xml-tree-values rows)
            (assoc :ltree-index (idx-fn)))
      ctx)))
