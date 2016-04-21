(ns vip.data-processor.validation.data-spec.v5-0
  (:require [vip.data-processor.validation.data-spec.value-format :as format]
            [vip.data-processor.validation.data-spec.coerce :as coerce]))

(def street-segments
  {:columns [{:name "id"}
             {:name "includes_all_addresses"
              :coerce coerce/postgres-boolean}
             {:name "address_direction"}
             {:name "city"}
             {:name "odd_even_both"}
             {:name "precinct_id"}
             {:name "start_house_number"
              :format format/all-digits
              :coerce coerce/coerce-integer}
             {:name "end_house_number"
              :format format/all-digits
              :coerce coerce/coerce-integer}
             {:name "state"}
             {:name "street_direction"}
             {:name "street_name"}
             {:name "street_suffix"}
             {:name "zip"}]})
