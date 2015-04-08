(ns vip.data-processor.validation.db.admin-addresses
  (:require [korma.core :as korma]))

(defn transform-field [address-type field-name]
  (keyword (str (name address-type)
                "_address_"
                (name field-name))))

(defn is-address-valid-fn [address-type]
  (let [required-fields (map (partial transform-field address-type)
                             [:line1 :city :state :zip])
        all-fields (map (partial transform-field address-type)
                        [:location_name :line1 :line2 :line3 :city :state :zip])]
    (fn [row]
      (let [address (select-keys row all-fields)]
        (or (every? empty? (vals address))
            (every? (complement empty?) (vals (select-keys address required-fields))))))))

(defn validate-address [rows ctx address-type]
  (let [bad-addresses (->> rows
                           (remove (is-address-valid-fn address-type))
                           (map :id))
        error-name (keyword (str "incomplete-" (name address-type) "-address"))]
    (if (seq bad-addresses)
      (assoc-in ctx [:errors "election_administration.txt" error-name]
                bad-addresses)
      ctx)))

(defn validate-addresses [ctx]
  (let [election-administrations (get-in ctx [:tables :election-administrations])
        rows (korma/select election-administrations
                           (korma/fields :id
                                         :physical_address_location_name
                                         :physical_address_line1
                                         :physical_address_line2
                                         :physical_address_line3
                                         :physical_address_city
                                         :physical_address_state
                                         :physical_address_zip
                                         :mailing_address_location_name
                                         :mailing_address_line1
                                         :mailing_address_line2
                                         :mailing_address_line3
                                         :mailing_address_city
                                         :mailing_address_state
                                         :mailing_address_zip))]
    (reduce (partial validate-address rows) ctx [:physical :mailing])))
