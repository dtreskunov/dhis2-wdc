(ns dhis2-wdc.core
  (:require-macros [cljs.core.async.macros :as async])
  (:require [dhis2-wdc.wdc :as wdc]
            [reagent.core :as r]
            [cljs-http.client :as http]
            [cljs.core.async :as async]))

(enable-console-print!)

(deftype DHIS2WDC []
    wdc/IWebDataConnector
    (-get-name [this]
      "DHIS2 Web Data Connector")
    (-get-table-infos [this]
      [{:id      "earthquakeFeed"
        :alias   "Earthquakes with magnitude greater than 4.5 in the last seven days"
        :columns [{:id       "id"
                   :dataType "string"}
                  {:id       "mag"
                   :alias    "magnitude"
                   :dataType "float"}
                  {:id       "title"
                   :dataType "string"}
                  {:id       "lat"
                   :dataType "float"}
                  {:id       "lon"
                   :dataType "float"}]}])
    (-get-standard-connections [this]
      [])
    (-get-rows [this table-info]
      (let [rows-chan (async/chan 1)
            url "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.geojson"
            response-chan (http/get url {:with-credentials? false})
            response->rows (fn [response]
                             (->> response
                                  :body
                                  :features
                                  (map (fn [feat]
                                         {:id    (feat :id)
                                          :mag   (get-in feat [:properties :mag])
                                          :title (get-in feat [:properties :title])
                                          :lon   (first (get-in feat [:geometry :coordinates]))
                                          :lat   (second (get-in feat [:geometry :coordinates]))}))))]
        (async/go
          (->> (async/<! response-chan)
               (response->rows)
               (async/>! rows-chan)))
        rows-chan))
    (-get-state [this])
    (-set-state [this state]))

(def *wdc* (DHIS2WDC.))
(wdc/register! *wdc*)

(defn root-component []
  [:input {:type "button" :value "Go!" :on-click #(wdc/go! *wdc*)}])

(r/render-component [root-component]
                          (. js/document (getElementById "app")))

(defn on-js-reload []
  ;; optionally touch your app-state to force rerendering depending on
  ;; your application
  ;; (swap! app-state update-in [:__figwheel_counter] inc)
)


