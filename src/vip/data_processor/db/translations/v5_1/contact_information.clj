(ns vip.data-processor.db.translations.v5-1.contact-information
  (:require [vip.data-processor.db.translations.util :as util]
            [clojure.string :as str]))

(defn contact-information->ltree
  ([] (contact-information->ltree "ContactInformation"))
  ([xml-element]
   (fn [idx-fn parent-path row]
     (when-not (every? #(str/blank? (get row %))
                       [:address_line_1
                        :ci_address_line_2
                        :ci_address_line_3
                        :ci_directions
                        :ci_email
                        :ci_fax
                        :ci_hours
                        :ci_hours_open_id
                        :ci_latitude
                        :ci_longitude
                        :ci_latlng_source
                        :ci_name
                        :ci_phone
                        :ci_uri])
       (let [index (idx-fn)
             base-path (str parent-path "." xml-element "." index)
             label-path (str base-path ".label")
             parent-with-id (util/id-path parent-path)
             sub-idx-fn (util/index-generator 0)]
         (conj
          (mapcat #(% sub-idx-fn base-path row)
                  [(util/simple-value->ltree :ci_address_line_1 "AddressLine" parent-with-id)
                   (util/simple-value->ltree :ci_address_line_2 "AddressLine" parent-with-id)
                   (util/simple-value->ltree :ci_address_line_3 "AddressLine" parent-with-id)
                   (util/internationalized-text->ltree :ci_directions "Directions" parent-with-id)
                   (util/simple-value->ltree :ci_email "Email" parent-with-id)
                   (util/simple-value->ltree :ci_fax "Fax" parent-with-id)
                   (util/internationalized-text->ltree :ci_hours "Hours" parent-with-id)
                   (util/simple-value->ltree :ci_hours_open_id "HoursOpenId" parent-with-id)
                   (util/latlng->ltree {:latitude :ci_latitude
                                        :longitude :ci_longitude
                                        :latlng_source :ci_latlng_source}
                                       parent-with-id)
                   (util/simple-value->ltree :ci_name "Name" parent-with-id)
                   (util/simple-value->ltree :ci_phone "Phone" parent-with-id)
                   (util/simple-value->ltree :ci_uri "Uri" parent-with-id)])
          {:path label-path
           :simple_path (util/path->simple-path label-path)
           :value (:ci_id row)
           :parent_with_id parent-with-id}))))))
