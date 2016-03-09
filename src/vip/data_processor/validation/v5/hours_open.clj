(ns vip.data-processor.validation.v5.hours-open
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.v5.util :as util]))

(defn valid-time-with-zone? [time]
  (re-matches
   #"(?:(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]|(?:24:00:00))(?:Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))"
   time))

(defn validate-times [{:keys [import-id] :as ctx}]
  (let [hours-open-path "VipObject.0.HoursOpen.*{1}.Schedule.*{1}.Hours.*{1}"
        start-times (util/select-path import-id
                                      (str hours-open-path ".StartTime.*{1}"))
        end-times (util/select-path import-id
                                    (str hours-open-path ".EndTime.*{1}"))
        invalid-times #(remove (comp valid-time-with-zone? :value) %)
        invalid-start-times (invalid-times start-times)
        invalid-end-times (invalid-times end-times)]
    (reduce (fn [ctx row]
              (update-in ctx
                         [:errors :hours-open (-> row :path .getValue) :format]
                         conj (:value row)))
            ctx (concat invalid-start-times invalid-end-times))))
