(ns vip.data-processor.db.translations.v5-1.sources
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.translations.util :as util]))

(defn row-fn [import-id]
  (korma/select (postgres/v5-1-tables :sources)
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
  (str "VipObject.0.Source." index))

(defn transform-fn [idx-fn row]
  (let [path (base-path (idx-fn))
        id-path (util/id-path path)
        child-idx-fn (util/index-generator 0)]
    (conj
     (mapcat #(% child-idx-fn path row)
             [(util/simple-value->ltree :date_time)
              (util/internationalized-text->ltree :description)
              (ci/contact-information->ltree "FeedContactInformation")
              (util/simple-value->ltree :name)
              (util/simple-value->ltree :organization_uri)
              (util/simple-value->ltree :terms_of_use_uri)
              (util/simple-value->ltree :vip_id)])
     {:path id-path
      :simple_path (util/path->simple-path id-path)
      :parent_with_id id-path
      :value (:id row)})))

(def transformer (util/transformer row-fn transform-fn))
