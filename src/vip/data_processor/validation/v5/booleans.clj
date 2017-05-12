(ns vip.data-processor.validation.v5.booleans
  (:require [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.validation.data-spec.value-format :as value-format]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.validation.xml.spec :as spec]
            [clojure.tools.logging :as log]))

(defn validate-format [{:keys [import-id] :as ctx}]
  (log/info "Validating booleans")
  (let [boolean-paths (spec/type->simple-paths "xs:boolean" "5.1")
        boolean-values (korma/select postgres/xml-tree-values
                          (korma/where {:results_id import-id
                                        :simple_path [in (map postgres/path->ltree boolean-paths)]}))]
      (reduce (fn [ctx row]
                (if (contains? (:check value-format/boolean-valid)
                               (:value row))
                  ctx
                  (let [parent-element-id (->(korma/exec-raw
                                               (:conn postgres/xml-tree-values)
                                               ["SELECT value
                                                  FROM xml_tree_values
                                                  WHERE path = subpath(text2ltree(?),0,4) || 'id'
                                                  and results_id = ?" [(-> row :path .getValue) import-id]]
                                               :results)
                                            first
                                            :value)]
                    (errors/v5-add-errors ctx
                                       :errors :boolean (-> row :path .getValue) :format parent-element-id
                                       (:value row)))))
              ctx boolean-values)))
