(ns dhis2-wdc.wdc
  (:require-macros [cljs.core.async.macros :as async])
  (:require [cljs.spec :as s]
            [cljs.core.async :as async]))

; (set! *warn-on-infer* true)

(s/check-asserts true)

;; Functions and objects of the WDC API v2 (http://tableau.github.io/webdataconnector/docs/api_ref)
(s/def ::id (s/and string? #(re-matches #"\w+" %)))
(s/def ::alias string?)
(s/def ::description string?)
(s/def ::incrementColumnId ::id)
(s/def ::joinOnly boolean?)
(s/def ::filterable boolean?)
(s/def ::dataType #{"bool" "date" "datetime" "float" "int" "string"})
(s/def ::aggType #{"avg" "count" "count_dist" "median" "sum"})
(s/def ::columnRole #{"dimension" "measure"})
(s/def ::columnType #{"continuous" "discrete"})
(s/def ::geoRole #{"area_code" "cbsa_msa" "city" "congressional_district" "country_region" "county" "state_province" "zip_code_postcode"})
(s/def ::numberFormat #{"currency" "number" "percentage" "scientific"})
(s/def ::unitsFormat #{"billions_english" "billions_standard" "millions" "thousands"})
(s/def ::foreignKey (s/keys :req-un [::tableId ::columnId]))
(s/def ::columnInfo (s/keys :req-un [::dataType
                                     ::id]
                            :opt-un [::aggType
                                     ::alias
                                     ::columnRole
                                     ::columnType
                                     ::description
                                     ::geoRole
                                     ::numberFormat
                                     ::unitsFormat
                                     ::filterable
                                     ::foreignKey]))
(s/def ::columns (s/coll-of ::columnInfo))
(s/def ::tableInfo (s/keys :req-un [::columns
                                    ::id]
                           :opt-un [::alias
                                    ::description
                                    ::incrementColumnId
                                    ::joinOnly]))
(s/def ::tableInfos (s/coll-of ::tableInfo))
(s/def ::standardConnection (s/keys :req-un [::alias
                                             ::tables
                                             ::joins]))
(s/def ::standardConnections (s/coll-of ::standardConnection))
(s/def ::phase #{"auth" "interactive" "gatherData"})
(s/def ::authType #{"none" "basic" "custom"})

(s/def ::state (s/keys :opt-un [::connection-data ::username ::password]))

(defprotocol IWebDataConnector
  "Web Data Connector protocol"
  (-get-auth-type [this] "AuthType")
  (-get-name [this] "Connection name")
  (-get-table-infos [this] "Array of TableInfo objects")
  (-get-standard-connections [this] "Array of StandardConnection objects (describing predefined table joins) (optional)")
  (-get-rows! [this rows-chan table-info inc-val filter-values] "Asynchronously put arrays of rows into the provided channel")
  (-shutdown [this] "Called when the current WDC phase ends. Must return ::state which needs to be persisted.")
  (-init [this phase state] "Called when a new WDC phase is entered. ::state saved at the end of previous phase is provided."))

(defn get-phase []
  "Returns the current WDC phase"
  (when-let [phase (.-phase js/tableau)]
    (s/assert ::phase phase)))

(defn- init [w callback]
  (let [phase (get-phase)
        connection-data-str (aget js/tableau "connectionData")
        username (aget js/tableau "username")
        password (aget js/tableau "password")
        state {:username username
               :password password
               :connection-data (when-not (empty? connection-data-str)
                                  (js->clj (.parse js/JSON connection-data-str) :keywordize-keys true))}
        auth-type (s/assert ::authType (-get-auth-type w))]
    (println (str "Entering phase: " phase))
    (-init w phase state)
    (aset js/tableau "authType" auth-type))
  (callback))

(defn- shutdown [w callback]
  (println (str "Exiting phase: " (get-phase)))
  (let [{:keys [username password connection-data]} (s/assert ::state (-shutdown w))
        connection-data-str (->> connection-data
                                 clj->js
                                 (.stringify js/JSON))]
    (aset js/tableau "connectionData" connection-data-str)
    (aset js/tableau "username" username)
    (aset js/tableau "password" password))
  (callback))

(defn- get-schema [w callback]
  (println "get-schema")
  (callback (clj->js (s/assert ::tableInfos (-get-table-infos w)))
            (clj->js (s/assert ::standardConnections (-get-standard-connections w)))))

(defn- get-data [w js-table callback]
  (println "get-data")
  (let [js-table-info (aget js-table "tableInfo")
        inc-val (aget js-table "incrementValue")
        filter-values (js->clj (aget js-table "filterValues"))
        append-rows (aget js-table "appendRows")
        table-info (s/assert ::tableInfo (js->clj js-table-info :keywordize-keys true))
        rows-chan (async/chan)]
    (-get-rows! w rows-chan table-info inc-val filter-values)
    (async/go-loop [total 0]
      (if-let [rows (async/<! rows-chan)]
        (do
          (append-rows (clj->js rows))
          (let [num (+ total (count rows))]
            (.reportProgress js/tableau (str (:alias table-info) ": " num " rows fetched"))
            (recur num)))
        (do
          (println "get-data DONE!")
          (callback))))))

(defn register! [w]
  "Registers the WDC"
  (let [connector
        (doto (.makeConnector js/tableau)
          (aset "init" (partial init w))
          (aset "shutdown" (partial shutdown w))
          (aset "getSchema" (partial get-schema w))
          (aset "getData" (partial get-data w)))]
    (.registerConnector js/tableau connector)))

(defn go! [w]
  "Transitions from WDC 'interactive' phase to 'gather data' phase"
  (shutdown w #(println "shutdown callback"))
  (aset js/tableau "connectionName" (-get-name w))
  (.submit js/tableau))
