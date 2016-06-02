(ns vip.data-processor.db.translations.v5-1.people
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.translations.util :as util]))

(defn row-fn [import-id]
  (korma/select (postgres/v5-1-tables :people)
    (korma/fields :*
                  [:contact_information.id :ci_id]
                  [:contact_information.address_line_1 :ci_address_line_1]
                  [:contact_information.address_line_2 :ci_address_line_2]
                  [:contact_information.address_line_3 :ci_address_line_3]
                  [:contact_information.directions :ci_directions]
                  [:contact_information.email :ci_email]
                  [:contact_information.fax :ci_fax]
                  [:contact_information.hours :ci_hours]
                  [:contact_information.hours_open_id :ci_hours_open_id]
                  [:contact_information.latitude :ci_latitude]
                  [:contact_information.longitude :ci_longitude]
                  [:contact_information.latlng_source :ci_latlng_source]
                  [:contact_information.name :ci_name]
                  [:contact_information.parent_id :ci_parent_id]
                  [:contact_information.phone :ci_phone]
                  [:contact_information.uri :ci_uri])
    (korma/join :left (postgres/v5-1-tables :contact-information)
                (and (= :contact_information.parent_id :id)
                     (= :contact_information.results_id :results_id)))
    (korma/where {:results_id import-id})))

(defn base-path [index]
  (str "VipObject.0.Person." index))

(defn transform-fn [idx-fn row]
  (let [path (base-path (idx-fn))
        id-path (util/id-path path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn path row)
             [ci/contact-information->ltree
              (util/simple-value->ltree :date_of_birth)
              (util/simple-value->ltree :first_name)
              (util/simple-value->ltree :gender)
              (util/simple-value->ltree :last_name)
              (util/simple-value->ltree :middle_name)
              (util/simple-value->ltree :nickname)
              (util/simple-value->ltree :party_id)
              (util/simple-value->ltree :prefix)
              (util/simple-value->ltree :profession)
              (util/simple-value->ltree :suffix)
              (util/simple-value->ltree :title)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id row)})))

(def transformer (util/transformer row-fn transform-fn))
