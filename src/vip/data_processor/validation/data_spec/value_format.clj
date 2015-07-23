(ns vip.data-processor.validation.data-spec.value-format)

(def all-digits
  {:check #"\A\d+\z"
   :message "Invalid data type"})

(def contest-election-type
  {:check ["general" "primary" "run-off" "referendum" "judge retention"]
   :message "Invalid election_type"})

(def date
  {:check #"\A\d{4}-\d{2}-\d{2}\z"
   :message "Invalid date format"})

(def datetime
  {:check #"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\z"
   :message "Invalid date format"})

(def election-type
  {:check ["federal" "state" "county" "city" "town"]
   :message "Invalid election_type"})

(def electoral-district-type
  {:check #"\A(?ix: state(?:wide)? |
                    u\.?s\.?\ senate |
                    u\.?s\.?\ (?:rep(?:resentative)?|house) |
                    congressional |
                    (?:[a-tv-z][a-tv-z]|ut|state)\ senate |
                    (?:[a-tv-z][a-rt-z]|ut|state)\ (?:rep(?:resentative)?|house(?:\ of\ delegates)?) |
                    house\ of\ delegates |
                    house |
                    county(?:wide)? |
                    county\ offices |
                    mayor |
                    municipality |
                    county\ commission(?:er)? |
                    ward |
                    school |
                    school\ district |
                    school\ member\ district |
                    local\ school\ board |
                    prosecutorial\ district |
                    superior\ court |
                    town(?:ship)? |
                    sanitary)\z"
   :message "Invalid type"})

(def email
  {:check #"\A.+@.+\..+\z"
   :message "Invalid email format"})

(def locality-type
  {:check ["county" "city" "town" "township" "borough" "parish" "village" "region"]
   :message "Invalid locality_type"})

(def odd-even-both
  {:check ["odd" "even" "both"]
   :message "Must be \"odd\", \"even\", or \"both\""})

(def phone
  {:check #"\A\(\d{3}\) \d{3}-\d{4}\z"
   :message "Invalid phone number format"})

(def street-direction
  {:check ["n" "s" "e" "w" "nw" "ne" "sw" "se"]
   :message "Invalid street direction"})

(def url
  {:check #"\Ahttps?://"
   :message "Invalid url format"})

(def yes-no
  {:check ["yes" "no"]
   :message "Must be yes or no"})
