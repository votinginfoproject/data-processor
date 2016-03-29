(ns vip.data-processor.validation.v5.electoral-district-test
  (:require [vip.data-processor.validation.v5.electoral-district :as v5.electoral-district]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (testing "missing Name is an error"
    (let [ctx {:input (xml-input "v5-electoral-districts.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.electoral-district/validate-no-missing-names)]
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

(deftest ^:postgres validate-no-missing-types-test
  (testing "missing Type is an error"
    (let [ctx {:input (xml-input "v5-electoral-districts.xml")}
          out-ctx (-> ctx
                      psql/start-run
                      xml/load-xml-ltree
                      v5.electoral-district/validate-no-missing-types)]
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
