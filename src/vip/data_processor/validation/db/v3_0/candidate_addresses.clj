(ns vip.data-processor.validation.db.v3-0.candidate-addresses
  (:require [clojure.string :as str]
            [com.climate.newrelic.trace :refer [defn-traced]]
            [korma.core :as korma]
            [vip.data-processor.errors :as errors]))

(def required-address-fields
  #{:filed_mailing_address_line1
    :filed_mailing_address_city
    :filed_mailing_address_state
    :filed_mailing_address_zip})

(defn valid-address?
  [address]
  (let [address' (dissoc address :id)]
    (cond
      ;; if all address fields are empty then is valid
      (not (seq (remove str/blank? (vals address'))))
      true

      ;; if there is an address with any required fields blank, then invalid
      (->> ((apply juxt required-address-fields) address')
           (some str/blank?))
      false

      ;; Otherwise we have a valid address with all required fields
      :else
      true)))

(defn validate-addresses'
  "Given a candidates-table, confirms that all the rows are correct in
  terms of having non-null/blank values for the fields as required by
  the spec:

  https://github.com/votinginfoproject/data-processor/blob/bce02bcd0e8be67c6211d0edcf3f2d92a3d8b26d/resources/specs/vip_spec_v3.0.xsd#L336
  "
  [candidates-table]
  (->> (korma/fields :id
                     :filed_mailing_address_location_name
                     :filed_mailing_address_state
                     :filed_mailing_address_line1
                     :filed_mailing_address_line3
                     :filed_mailing_address_line2
                     :filed_mailing_address_zip
                     :filed_mailing_address_city)
       (korma/select candidates-table)
       (remove valid-address?)))

(defn-traced validate-addresses
  [ctx]
  (let [bad-addresses (validate-addresses' (get-in ctx [:tables :candidates]))]
    (if (seq bad-addresses)
      (reduce
       (fn [ctx {:keys [id]}]
         ;; maybe this function should take a map
         (errors/add-errors
          ctx :critical :candidates id
          :incomplete-candidate-address "Incomplete address"))
       ctx bad-addresses)
      ctx)))
