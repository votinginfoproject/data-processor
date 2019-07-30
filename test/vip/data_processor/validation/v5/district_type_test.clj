(ns vip.data-processor.validation.v5.district-type-test
  (:require [vip.data-processor.validation.v5.district-type :as v5.district-type]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-test
  (testing "Locality elements"
    (let [errors-chan (a/chan 100)
          ctx {:xml-source-file-path (xml-input "v5-localities.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.district-type/validate)
          errors (all-errors errors-chan)]
      (testing "type missing is OK"
        (assert-no-problems errors
                            {:severity :errors
                             :scope :locality
                             :identifier "VipObject.0.Locality.0.Type"
                             :error-type :missing}))
      (testing "type present and valid is OK"
        (assert-no-problems errors
                            {:severity :errors
                             :scope :locality
                             :identifier "VipObject.0.Locality.1.Type.2"
                             :error-type :format}))
      (testing "type present and invalid is an error"
        (is (contains-error? errors
                             {:severity :errors
                              :scope :locality
                              :identifier "VipObject.0.Locality.2.Type.2"
                              :error-type :format})))))
  (testing "ElectoralDistrict elements"
    (let [errors-chan (a/chan 100)
          ctx {:xml-source-file-path (xml-input "v5-electoral-districts.xml")
               :errors-chan errors-chan}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.district-type/validate)
          errors (all-errors errors-chan)]
      (testing "type valid is OK"
        (are [path]
            (assert-no-problems errors
                                {:severity :errors
                                 :scope :electoral-district
                                 :identifier path
                                 :error-type :format})
          "VipObject.0.ElectoralDistrict.0.Type.1"
          "VipObject.0.ElectoralDistrict.1.Type.1"
          "VipObject.0.ElectoralDistrict.4.Type.0"))
      (testing "type invalid is an error"
        (are [path]
            (is (contains-error? errors
                                 {:severity :errors
                                  :scope :electoral-district
                                  :identifier path
                                  :error-type :format}))
          "VipObject.0.ElectoralDistrict.2.Type.1"
          "VipObject.0.ElectoralDistrict.3.Type.1")))))
