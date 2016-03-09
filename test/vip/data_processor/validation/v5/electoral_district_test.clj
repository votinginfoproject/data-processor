(ns vip.data-processor.validation.v5.electoral-district-test
  (:require [vip.data-processor.validation.v5.electoral-district :as v5.electoral-district]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-name-test
  (testing "missing Name is an error"
    (let [ctx {:input (xml-input "v5-electoral-districts.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.electoral-district/validate-name)]
      (is (get-in out-ctx [:errors :electoral-district
                           "VipObject.0.ElectoralDistrict.4.Name" :missing]))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.0.Name"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.1.Name"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.2.Name"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.3.Name"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.5.Name"]))))))

(deftest ^:postgres validate-type-test
  (testing "missing Type is an error"
    (let [ctx {:input (xml-input "v5-electoral-districts.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.electoral-district/validate-type)]
      (is (get-in out-ctx [:errors :electoral-district
                           "VipObject.0.ElectoralDistrict.5.Type" :missing]))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.0.Type"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.1.Type"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.2.Type"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.3.Type"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.4.Type"]))))))

(deftest ^:postgres validate-type-format-test
  (testing "invalid Type is an error"
    (let [ctx {:input (xml-input "v5-electoral-districts.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.electoral-district/validate-type-format)]
      (is (get-in out-ctx [:errors :electoral-district
                           "VipObject.0.ElectoralDistrict.2.Type.1" :format]))
      (is (get-in out-ctx [:errors :electoral-district
                           "VipObject.0.ElectoralDistrict.3.Type.1" :format]))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.0.Type.1"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.1.Type.1"])))
      (is (not (get-in out-ctx [:errors :electoral-district
                                "VipObject.0.ElectoralDistrict.4.Type.0"]))))))
