(ns vip.data-processor.validation.v5.boolean
  (:require [korma.core :as korma]
            [vip-data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.data-spec.value-format :as value-format]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.validation.xml.spec :as spec]
            [clojure.tools.logging :as log]))

(defn validate-booleans [{:keys [import-id] :as ctx}]
  (log/info "Validating booleans")
  (let [boolean-paths (spec/type->simple-paths "xs:boolean" "5.1")
        boolean-values (korma/select postgres/xml-tree-values
                          (korma/where {:results_id import-id
                                        :simple_path [in (map postgres/path->ltree boolean-paths)]}))]
      (reduce (fn [ctx row]
                (if (re-find (:check value-format/boolean)
                             (:value row))
                  ctx
                  (errors/add-errors ctx
                                     :errors :boolean (-> row :path .getValue) :format
                                     (:value row))))
              ctx boolean-values)))
