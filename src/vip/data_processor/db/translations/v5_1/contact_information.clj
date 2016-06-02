(ns vip.data-processor.db.translations.v5-1.contact-information
  (:require [vip.data-processor.db.translations.util :as util]
            [clojure.string :as str]))

(defn contact-information->ltree [idx-fn parent-path row]
  (when-not (every? #(str/blank? (get row %))
                    [:address_line_1
                     :address_line_2
                     :address_line_3
                     :directions
                     :email
                     :fax
                     :hours
                     :hours_open_id
                     :latitude
                     :longitude
                     :latlng_source
                     :name
                     :phone
                     :uri])
    (let [index (idx-fn)
          base-path (str parent-path ".ContactInformation." index)
          label-path (str base-path ".label")
          parent-with-id (util/id-path parent-path)
          sub-idx-fn (util/index-generator 0)]
      (conj
       (mapcat #(% sub-idx-fn base-path row)
               [(util/simple-value->ltree :address_line_1 "AddressLine" parent-with-id)
                (util/simple-value->ltree :address_line_2 "AddressLine" parent-with-id)
                (util/simple-value->ltree :address_line_3 "AddressLine" parent-with-id)
                (util/internationalized-text->ltree :directions parent-with-id)
                (util/simple-value->ltree :email "Email" parent-with-id)
                (util/simple-value->ltree :fax "Fax" parent-with-id)
                (util/internationalized-text->ltree :hours parent-with-id)
                (util/simple-value->ltree :hours_open_id "HoursOpenId" parent-with-id)
                (util/latlng->ltree parent-with-id)
                (util/simple-value->ltree :name "Name" parent-with-id)
                (util/simple-value->ltree :phone "Phone" parent-with-id)
                (util/simple-value->ltree :uri "Uri" parent-with-id)])
       {:path label-path
        :simple_path (util/path->simple-path label-path)
        :value (:id row)
        :parent_with_id parent-with-id}))))
