(ns vip.data-processor.output.v3-0.address
  (:require [vip.data-processor.output.xml-helpers :refer :all]))

(defn address-parts [address-type object]
  (letfn [(address-key [address-part]
            (keyword (str (name address-type) "_" (name address-part))))]
    (->> [:location_name :line1 :line2 :line3 :city :state :zip
          :house_number :house_number_prefix :house_number_suffix
          :street_direction :street_name :street_suffix :address_direction
          :apartment]
         (map (fn [address-part]
                {:tag address-part :content [(str (get object (address-key address-part)))]}))
         remove-empties)))

(defn address [address-type object]
  (let [parts (address-parts address-type object)]
    (when-not (empty? parts)
      {:tag address-type :content parts})))
