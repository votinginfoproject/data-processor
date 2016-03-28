(ns vip.data-processor.validation.v5.electoral-district
  (:require [vip.data-processor.validation.v5.util :as util]))

(def validate-no-missing-names
  (util/validate-no-missing-elements :electoral-district :name))

(def validate-no-missing-types
  (util/validate-no-missing-elements :electoral-district :type))

(def types
  #{"city" "city-council" "congressional" "county" "county-council" "judicial"
    "municipality" "national" "school" "special" "state" "state-house"
    "state-senate" "town" "township" "utility" "village" "ward" "water"
    "other"})

(defn valid-type? [type] (types type))

(defn validate-type-formats [{:keys [import-id] :as ctx}]
  (let [imported-types (util/select-lquery
                        import-id
                        "VipObject.0.ElectoralDistrict.*{1}.Type.*{1}")
        invalid-types (remove (comp valid-type? :value) imported-types)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :electoral-district (-> row :path .getValue)
                          :format] conj (:value row)))
            ctx invalid-types)))
