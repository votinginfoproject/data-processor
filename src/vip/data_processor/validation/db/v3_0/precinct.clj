(ns vip.data-processor.validation.db.v3-0.precinct
  (:require [korma.core :as korma]))

(defn validate-no-missing-polling-locations
  "Precincts are missing a polling location if they are not mail only
  and don't have a reference in precinct_polling_locations."
  [ctx]
  (let [{:keys [precincts precinct-polling-locations]} (:tables ctx)
        bad-precincts (korma/select
                       precincts
                       (korma/fields :id)
                       (korma/where (or {:mail_only 0}
                                        {:mail_only nil}))
                       (korma/where {:id [not-in
                                          (korma/subselect
                                           precinct-polling-locations
                                           (korma/modifier "DISTINCT")
                                           (korma/fields :precinct_id))]}))]
    (reduce (fn [ctx bad-precinct-row]
              (assoc-in ctx [:warnings :precincts
                             (:id bad-precinct-row) :missing-polling-location]
                        ["Missing polling location"]))
            ctx bad-precincts)))
