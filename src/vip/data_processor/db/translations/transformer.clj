(ns vip.data-processor.db.translations.transformer
  (:require
   [vip.data-processor.db.translations.v5-1.ballot-measure-contests :as ballot-measure-contests]
   [vip.data-processor.db.translations.v5-1.ballot-measure-selections :as ballot-measure-selections]
   [vip.data-processor.db.translations.v5-1.ballot-selections :as ballot-selections]
   [vip.data-processor.db.translations.v5-1.candidate-contests :as candidate-contests]
   [vip.data-processor.db.translations.v5-1.candidate-selections :as candidate-selections]
   [vip.data-processor.db.translations.v5-1.candidates :as candidates]
   [vip.data-processor.db.translations.v5-1.contests :as contests]
   [vip.data-processor.db.translations.v5-1.election :as election]
   [vip.data-processor.db.translations.v5-1.election-administrations :as election-administrations]
   [vip.data-processor.db.translations.v5-1.electoral-districts :as electoral-districts]
   [vip.data-processor.db.translations.v5-1.hours-open :as hours-open]
   [vip.data-processor.db.translations.v5-1.localities :as localities]
   [vip.data-processor.db.translations.v5-1.offices :as offices]
   [vip.data-processor.db.translations.v5-1.ordered-contests :as ordered-contests]
   [vip.data-processor.db.translations.v5-1.parties :as parties]
   [vip.data-processor.db.translations.v5-1.party-contests :as party-contests]
   [vip.data-processor.db.translations.v5-1.party-selections :as party-selections]
   [vip.data-processor.db.translations.v5-1.people :as people]
   [vip.data-processor.db.translations.v5-1.polling-locations :as polling-locations]
   [vip.data-processor.db.translations.v5-1.precincts :as precincts]
   [vip.data-processor.db.translations.v5-1.retention-contests :as retention-contests]
   [vip.data-processor.db.translations.v5-1.sources :as sources]
   [vip.data-processor.db.translations.v5-1.states :as states]
   [vip.data-processor.db.translations.v5-1.street-segments :as street-segments]))

(def transformers
  [sources/transformer
   election/transformer
   states/transformer
   ballot-measure-contests/transformer
   ballot-measure-selections/transformer
   ballot-selections/transformer
   candidate-contests/transformer
   candidate-selections/transformer
   candidates/transformer
   contests/transformer
   election-administrations/transformer
   electoral-districts/transformer
   hours-open/transformer
   localities/transformer
   offices/transformer
   ordered-contests/transformer
   parties/transformer
   party-contests/transformer
   party-selections/transformer
   people/transformer
   polling-locations/transformer
   precincts/transformer
   retention-contests/transformer
   street-segments/transformer])
