(ns vip.data-processor.db.translations.v5-1.departments
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.util :as util]
            [vip.data-processor.db.translations.v5-1.contact-information :as ci]
            [vip.data-processor.db.translations.v5-1.voter-services :as vs]))

(defn row-fn
  [import-id]
   (korma/select (postgres/v5-1-tables :departments)
     (korma/fields :*
                   [:contact_information.id :ci_id]
                   [:contact_information.email :ci_email]
                   [:contact_information.fax :ci_fax]
                   [:contact_information.hours :ci_hours]
                   [:contact_information.hours_open_id :ci_hours_open_id]
                   [:contact_information.name :ci_name]
                   [:contact_information.uri :ci_uri]
                   [:contact_information.directions :ci_directions]
                   [:contact_information.address_line_1 :ci_address_line_1]
                   [:contact_information.address_line_2 :ci_address_line_2]
                   [:contact_information.address_line_3 :ci_address_line_3]
                   [:contact_information.latitude :ci_latitude]
                   [:contact_information.longitude :ci_longitude]
                   [:contact_information.latlng_source :ci_latlng_source]
                   [:contact_information.phone :ci_phone]
                   [:contact_information.parent_id :ci_parent_id])
     (korma/join :left (postgres/v5-1-tables :contact-information)
                 (and (= :contact_information.parent_id :id)
                      (= :contact_information.results_id :results_id)))
     (korma/where {:results_id import-id})))

(defn departments->ltree [departments voter-services parent-with-id]
  (fn [idx-fn parent-path _]
    (mapcat
     (fn [department]
       (let [path (str parent-path ".Department." (idx-fn))
             child-idx-fn (util/index-generator 0)
             vss (get voter-services (:id department))]
         (mapcat
          #(% child-idx-fn path department)
          [(ci/contact-information->ltree "ContactInformation" parent-with-id)
           (util/simple-value->ltree :election_official_person_id
                                     "ElectionOfficialPersonId"
                                     parent-with-id)
           (vs/voter-service->ltree vss parent-with-id)])))
     departments)))
