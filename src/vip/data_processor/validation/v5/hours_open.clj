(ns vip.data-processor.validation.v5.hours-open
  (:require [vip.data-processor.validation.v5.util :as util]
            [clojure.tools.logging :as log]))

(defn valid-time-with-zone? [time]
  (re-matches
   #"\A(?:(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]|(?:24:00:00))(?:Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))\z"
   time))

(defn validate-times [{:keys [import-id] :as ctx}]
  (log/info "Validating times")
  (let [hours-open-path "VipObject.0.HoursOpen.*{1}.Schedule.*{1}.Hours.*{1}"
        times (util/select-lquery
               import-id
               (str hours-open-path ".StartTime|EndTime.*{1}"))
        invalid-times (remove (comp valid-time-with-zone? :value) times)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :hours-open (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx invalid-times)))
