(ns dhis2-wdc.core
  (:require-macros [cljs.core.async.macros :as async])
  (:require [dhis2-wdc.wdc :as wdc]
            [reagent.core :as r]
            [cljs-http.client :as http]
            [cljs.core.async :as async]))

(enable-console-print!)
(declare render)

(defn response->rows [response]
  (->> response
       :body
       :features
       (map (fn [feat]
              {:id    (feat :id)
               :mag   (get-in feat [:properties :mag])
               :title (get-in feat [:properties :title])
               :lon   (first (get-in feat [:geometry :coordinates]))
               :lat   (second (get-in feat [:geometry :coordinates]))}))))

(deftype DHIS2WDC [state]
    wdc/IWebDataConnector
    (-get-name [this]
      (:name @state))
    (-get-table-infos [this]
      [(:table-info @state)])
    (-get-standard-connections [this]
      [])
    (-get-rows [this table-info]
      (let [rows (async/chan 1)
            url (:url @state)
            response (http/get url {:with-credentials? false})]
        (async/go
          (->> (async/<! response)
               (response->rows)
               (async/>! rows)))
        rows))
    (-shutdown [this]
      @state)
    (-init [this phase a-state]
      (swap! state merge a-state)
      (when (= "interactive" phase)
        (render))))

(defn make-state []
  {:name "Web Data Connector"
   :url "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.geojson"
   :table-info {:id      "earthquakeFeed"
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
                           :dataType "float"}]}})

(def app-state (r/atom (make-state)))
(def wdc (DHIS2WDC. app-state))
(wdc/register! wdc)

(defn root-component []
  [:input {:type "button" :value "Go!" :on-click #(wdc/go! wdc)}])

(defn render []
  (r/render-component [root-component]
                      (. js/document (getElementById "app"))))

(defn on-js-reload []
  ;; optionally touch your app-state to force rerendering depending on
  ;; your application
  ;; (swap! app-state update-in [:__figwheel_counter] inc)
)


