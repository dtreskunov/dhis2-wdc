(ns dhis2-wdc.wdc
  (:require-macros [cljs.core.async.macros :as async])
  (:require [cljs.spec :as s]
            [cljs.core.async :as async]))

(s/check-asserts true)

;; Functions and objects of the WDC API v2 (http://tableau.github.io/webdataconnector/docs/api_ref)
(s/def ::id string?)
(s/def ::alias string?)
(s/def ::description string?)
(s/def ::incrementColumnId string?)
(s/def ::dataType #{"bool" "date" "datetime" "float" "int" "string"})
(s/def ::aggType #{"avg" "count" "count_dist" "median" "sum"})
(s/def ::columnRole #{"dimension" "measure"})
(s/def ::columnType #{"continuous" "discrete"})
(s/def ::geoRole #{"area_code" "cbsa_msa" "city" "congressional_district" "country_region" "county" "state_province" "zip_code_postcode"})
(s/def ::numberFormat #{"currency" "number" "percentage" "scientific"})
(s/def ::unitsFormat #{"billions_english" "billions_standard" "millions" "thousands"})
(s/def ::columnInfo (s/keys :req-un [::dataType
                                     ::id]
                            :opt-un [::aggType
                                     ::alias
                                     ::columnRole
                                     ::columnType
                                     ::description
                                     ::geoRole
                                     ::numberFormat
                                     ::unitsFormat]))
(s/def ::columns (s/coll-of ::columnInfo))
(s/def ::tableInfo (s/keys :req-un [::columns
                                    ::id]
                           :opt-un [::alias
                                    ::description
                                    ::incrementColumnId]))
(s/def ::tableInfos (s/coll-of ::tableInfo))
(s/def ::standardConnection (s/keys :req-un [::alias
                                             ::tables
                                             ::joins]))
(s/def ::standardConnections (s/coll-of ::standardConnection))
(s/def ::phase #{"auth" "interactive" "gatherData"})

(defprotocol IWebDataConnector
  "Web Data Connector protocol"
  (-get-name [this] "Connection name")
  (-get-table-infos [this] "Array of TableInfo objects")
  (-get-standard-connections [this] "Array of StandardConnection objects (describing predefined table joins) (optional)")
  (-get-rows [this table-info] "core.async/channel containing retrieved table rows")
  (-shutdown [this] "Called when the current WDC phase ends. Must return state which need to be persisted.")
  (-init [this phase state] "Called when a new WDC phase is entered. State saved at the end of previous phase is provided."))

(defn get-phase []
  "Returns the current WDC phase"
  (when-let [phase (.-phase js/tableau)]
    (s/assert ::phase phase)))

(defn- init [w callback]
  (let [phase (get-phase)
        s (aget js/tableau "connectionData")
        state (if-not (empty? s) (js->clj (.parse js/JSON s) :keywordize-keys true))]
    (println (str "Entering phase: " phase))
    (-init w phase state))
  (callback))

(defn- shutdown [w callback]
  (println (str "Exiting phase: " (get-phase)))
  (when-let [state (-shutdown w)]
    (->> state
         clj->js
         (.stringify js/JSON)
         (aset js/tableau "connectionData")))
  (callback))

(defn- get-schema [w callback]
  (println "get-schema")
  (callback (clj->js (s/assert ::tableInfos (-get-table-infos w)))
            (clj->js (s/assert ::standardConnections (-get-standard-connections w)))))

(defn- get-data [w js-table callback]
  (println "get-data")
  (let [js-table-info (.-tableInfo js-table)
        table-info (s/assert ::tableInfo (js->clj js-table-info :keywordize-keys true))
        rows-chan (-get-rows w table-info)]
    (async/go-loop []
      (when-let [rows (async/<! rows-chan)]
        (.appendRows js-table (clj->js rows))
        (recur)))
    (callback)))

(defn register! [w]
  "Registers the WDC"
  (let [connector
        (doto (.makeConnector js/tableau)
          (aset "init" (partial init w))
          (aset "shutdown" (partial shutdown w))
          (aset "getSchema" (partial get-schema w))
          (aset "getData" (partial get-data w)))]
    (.registerConnector js/tableau connector))
  (aset js/tableau "connectionName" (-get-name w)))

(defn go! [w]
  "Transitions from WDC 'interactive' phase to 'gather data' phase"
  (shutdown w #(println "shutdown callback"))
  (.submit js/tableau))