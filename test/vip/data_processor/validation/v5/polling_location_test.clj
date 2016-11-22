(ns vip.data-processor.validation.v5.polling-location-test
  (:require [vip.data-processor.validation.v5.polling-location :as v5.polling-location]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-address-lines-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-polling-locations.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.polling-location/validate-no-missing-address-lines)
        errors (all-errors errors-chan)]
    (testing "address-line missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :polling-location
                            :identifier "VipObject.0.PollingLocation.0.AddressLine"
                            :error-type :missing})))
    (testing "address-line present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :polling-location
                           :identifier "VipObject.0.PollingLocation.1.AddressLine"
                           :error-type :missing}))))

(deftest ^:postgres validate-latitude-longitude-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-polling-locations.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.polling-location/validate-no-missing-latitudes
                    v5.polling-location/validate-no-missing-longitudes
                    v5.polling-location/validate-latitude
                    v5.polling-location/validate-longitude)
        errors (all-errors errors-chan)]
    (testing "lat-lng missing is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :lat-lng
                           :identifier "VipObject.0.PollingLocation.0.LatLng"
                           :error-type :missing})
      (assert-no-problems errors
                          {:severity :errors
                           :scope :lat-lng
                           :identifier "VipObject.0.PollingLocation.1.LatLng"
                           :error-type :missing}))
    (testing "valid lat-lng is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :lat-lng
                           :identifier "VipObject.0.PollingLocation.2.LatLng.1.Latitude.0"
                           :error-type :format})
      (assert-no-problems errors
                          {:severity :errors
                           :scope :lat-lng
                           :identifier "VipObject.0.PollingLocation.2.LatLng.1.Latitude.1"
                           :error-type :format}))
    (testing "missing latitude is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :lat-lng
                            :identifier "VipObject.0.PollingLocation.3.LatLng.1.Latitude"
                            :error-type :missing})))
    (testing "missing longitude is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :lat-lng
                            :identifier "VipObject.0.PollingLocation.4.LatLng.1.Longitude"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :lat-lng
                            :identifier "VipObject.0.PollingLocation.5.LatLng.1.Longitude"
                            :error-type :missing})))
    (testing "invalid latitude is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :lat-lng
                            :identifier "VipObject.0.PollingLocation.5.LatLng.1.Latitude.0"
                            :error-type :format})))))
