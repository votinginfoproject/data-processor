(ns vip.data-processor.db.translations.v5-1.voter-services
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.translations.util :as util]))

(defn row-fn [import-id]
  (korma/select (postgres/v5-1-tables :voter-services)
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

(defn voter-service->ltree [vss parent-with-id]
  (fn [idx-fn parent-path _]
    (mapcat (fn [vs]
              (let [path (str parent-path ".VoterService." (idx-fn))
                    label-path (str path ".label")
                    sub-idx-fn (util/index-generator 0)]
                (conj
                 (mapcat #(% sub-idx-fn path vs)
                         [(ci/contact-information->ltree)
                          (util/internationalized-text->ltree :description)
                          (util/simple-value->ltree :election_official_person_id)
                          (util/simple-value->ltree :type)
                          (util/simple-value->ltree :other_type)])
                 {:path label-path
                  :simple_path (util/path->simple-path label-path)
                  :value (:id vs)
                  :parent_with_id parent-with-id})))
            vss)))
