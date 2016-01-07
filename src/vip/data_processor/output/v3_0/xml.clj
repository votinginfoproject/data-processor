(ns vip.data-processor.output.v3-0.xml
  (:require [vip.data-processor.output.v3-0.ballot :as ballot]
            [vip.data-processor.output.v3-0.ballot-line-result :as ballot-line-result]
            [vip.data-processor.output.v3-0.ballot-response :as ballot-response]
            [vip.data-processor.output.v3-0.candidate :as candidate]
            [vip.data-processor.output.v3-0.contest :as contest]
            [vip.data-processor.output.v3-0.contest-result :as contest-result]
            [vip.data-processor.output.v3-0.custom-ballot :as custom-ballot]
            [vip.data-processor.output.v3-0.early-vote-site :as early-vote-site]
            [vip.data-processor.output.v3-0.election :as election]
            [vip.data-processor.output.v3-0.election-administration :as election-administration]
            [vip.data-processor.output.v3-0.election-official :as election-official]
            [vip.data-processor.output.v3-0.electoral-district :as electoral-district]
            [vip.data-processor.output.v3-0.locality :as locality]
            [vip.data-processor.output.v3-0.polling-location :as polling-location]
            [vip.data-processor.output.v3-0.precinct :as precinct]
            [vip.data-processor.output.v3-0.precinct-split :as precinct-split]
            [vip.data-processor.output.v3-0.referendum :as referendum]
            [vip.data-processor.output.v3-0.source :as source]
            [vip.data-processor.output.v3-0.state :as state]
            [vip.data-processor.output.v3-0.street-segment :as street-segment]))

(def xml-node-fns [ballot/xml-nodes
                   ballot-line-result/xml-nodes
                   ballot-response/xml-nodes
                   candidate/xml-nodes
                   contest/xml-nodes
                   contest-result/xml-nodes
                   custom-ballot/xml-nodes
                   early-vote-site/xml-nodes
                   election/xml-nodes
                   election-administration/xml-nodes
                   election-official/xml-nodes
                   electoral-district/xml-nodes
                   locality/xml-nodes
                   polling-location/xml-nodes
                   precinct/xml-nodes
                   precinct-split/xml-nodes
                   referendum/xml-nodes
                   source/xml-nodes
                   state/xml-nodes
                   street-segment/xml-nodes])

(def vip-object-attrs
  {:xmlns:xsi "http://www.w3.org/2001/XMLSchema-instance"
   :xsi:noNamespaceSchemaLocation "http://election-info-standard.googlecode.com/files/election_spec_v3.0.xsd"
   :schemaVersion "3.0"})

(def vip-object
  {:tag :vip_object
   :attrs vip-object-attrs
   :content []})
