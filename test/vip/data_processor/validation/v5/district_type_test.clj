(ns vip.data-processor.validation.v5.district-type-test
  (:require [vip.data-processor.validation.v5.district-type :as v5.district-type]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-test
  (testing "Locality elements"
    (let [ctx {:input (xml-input "v5-localities.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.district-type/validate)]
      (testing "type missing is OK"
        (is (not (get-in out-ctx [:errors :locality
                                  "VipObject.0.Locality.0.Type" :missing]))))
      (testing "type present and valid is OK"
        (is (not (get-in out-ctx [:errors :locality
                                  "VipObject.0.Locality.1.Type.2" :format]))))
      (testing "type present and invalid is an error"
        (is (get-in out-ctx [:errors :locality
                             "VipObject.0.Locality.2.Type.2" :format])))))
  (testing "ElectoralDistrict elements"
    (let [ctx {:input (xml-input "v5-electoral-districts.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.district-type/validate)]
      (testing "type valid is OK"
        (is (not (get-in out-ctx [:errors :electoral-district
                                  "VipObject.0.ElectoralDistrict.0.Type.1" :format])))
        (is (not (get-in out-ctx [:errors :electoral-district
                                  "VipObject.0.ElectoralDistrict.1.Type.1" :format])))
        (is (not (get-in out-ctx [:errors :electoral-district
                                  "VipObject.0.ElectoralDistrict.4.Type.0" :format]))))
      (testing "type invalid is an error"
        (is (get-in out-ctx [:errors :electoral-district
                             "VipObject.0.ElectoralDistrict.2.Type.1" :format]))
        (is (get-in out-ctx [:errors :electoral-district
                             "VipObject.0.ElectoralDistrict.3.Type.1" :format]))))))
