(ns vip.data-processor.validation.v5.electoral-district-test
  (:require [vip.data-processor.validation.v5.electoral-district :as v5.electoral-district]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (testing "missing Name is an error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-electoral-districts.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.electoral-district/validate-no-missing-names)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :electoral-district
                            :identifier "VipObject.0.ElectoralDistrict.4.Name"
                            :error-type :missing}))
      (are [path]
          (assert-no-problems-2 errors
                                {:severity :errors
                                 :scope :electoral-district
                                 :identifier path})
        "VipObject.0.ElectoralDistrict.0.Name"
        "VipObject.0.ElectoralDistrict.1.Name"
        "VipObject.0.ElectoralDistrict.2.Name"
        "VipObject.0.ElectoralDistrict.3.Name"
        "VipObject.0.ElectoralDistrict.5.Name"))))

(deftest ^:postgres validate-no-missing-types-test
  (testing "missing Type is an error"
    (let [errors-chan (a/chan 100)
          ctx {:input (xml-input "v5-electoral-districts.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.electoral-district/validate-no-missing-types)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :electoral-district
                            :identifier "VipObject.0.ElectoralDistrict.5.Type"
                            :error-type :missing}))
      (are [path]
          (assert-no-problems-2 errors
                                {:severity :errors
                                 :scope :electoral-district
                                 :identifier path})
        "VipObject.0.ElectoralDistrict.0.Type"
        "VipObject.0.ElectoralDistrict.1.Type"
        "VipObject.0.ElectoralDistrict.2.Type"
        "VipObject.0.ElectoralDistrict.3.Type"
        "VipObject.0.ElectoralDistrict.4.Type"))))
