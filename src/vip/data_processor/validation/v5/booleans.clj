(ns vip.data-processor.validation.v5.booleans
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.data-spec.value-format :as value-format]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.validation.xml.spec :as spec]
            [clojure.tools.logging :as log]
            [vip.data-processor.validation.v5.util :as util]))

(defn validate-format [{:keys [import-id] :as ctx}]
  (log/info "Validating booleans")
  (let [boolean-paths (spec/type->simple-paths "xs:boolean" "5.2")
        boolean-values (korma/select postgres/xml-tree-values
                          (korma/where {:results_id import-id
                                        :simple_path [in (map postgres/path->ltree boolean-paths)]}))]
      (reduce (fn [ctx row]
                (if (contains? (:check value-format/boolean-valid)
                               (:value row))
                  ctx
                  (let [path (-> row :path .getValue)
                        parent-element-id (util/get-parent-element-id path import-id)]
                    (errors/add-v5-errors ctx
                                       :errors :boolean path :format parent-element-id
                                       (:value row)))))
              ctx boolean-values)))
