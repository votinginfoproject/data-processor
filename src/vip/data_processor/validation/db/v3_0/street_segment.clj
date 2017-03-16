(ns vip.data-processor.validation.db.v3-0.street-segment
  (:require [korma.core :as korma]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [vip.data-processor.errors :as errors]))

(defn query-overlaps [street-segments]
  (korma/select [street-segments :street_segments]
                (korma/database (:db street-segments))
                (korma/fields :street_segments.id :street_segments2.id)
                (korma/join :inner [street-segments :street_segments2]
                            (and (>= :street_segments.start_house_number :street_segments2.start_house_number)
                                 (<= :street_segments.start_house_number :street_segments2.end_house_number)
                                 (= :street_segments.non_house_address_street_name :street_segments2.non_house_address_street_name)
                                 (= (korma/sqlfn ifnull :street_segments.non_house_address_street_suffix "NULL_VALUE")
                                    (korma/sqlfn ifnull :street_segments2.non_house_address_street_suffix "NULL_VALUE"))
                                 (= (korma/sqlfn ifnull :street_segments.non_house_address_street_direction "NULL_VALUE")
                                    (korma/sqlfn ifnull :street_segments2.non_house_address_street_direction "NULL_VALUE"))
                                 (= (korma/sqlfn ifnull :street_segments.non_house_address_address_direction "NULL_VALUE")
                                    (korma/sqlfn ifnull :street_segments2.non_house_address_address_direction "NULL_VALUE"))
                                 (or (not= (korma/sqlfn ifnull :street_segments.precinct_id "NULL_VALUE")
                                           (korma/sqlfn ifnull :street_segments2.precinct_id "NULL_VALUE"))
                                     (not= (korma/sqlfn ifnull :street_segments.precinct_split_id "NULL_VALUE")
                                           (korma/sqlfn ifnull :street_segments2.precinct_split_id "NULL_VALUE")))
                                 (= :street_segments.non_house_address_city :street_segments2.non_house_address_city)
                                 (= :street_segments.non_house_address_zip :street_segments2.non_house_address_zip)
                                 (or (= :street_segments.odd_even_both :street_segments2.odd_even_both)
                                     (= :street_segments.odd_even_both "both")
                                     (= :street_segments2.odd_even_both "both")
                                     (= :street_segments.odd_even_both "")
                                     (= :street_segments2.odd_even_both ""))
                                 (not= :street_segments.id :street_segments2.id)))))

(defn-traced validate-no-overlapping-street-segments [ctx]
  (let [street-segments (get-in ctx [:tables :street-segments])
        overlaps (->> street-segments
                      query-overlaps
                      (map vals)
                      (map set)
                      set)]
    (reduce (fn [ctx overlap]
              (let [[id overlap-id] (sort overlap)]
                (errors/add-errors ctx :errors :street-segments id :overlaps
                                   overlap-id)))
            ctx overlaps)))
