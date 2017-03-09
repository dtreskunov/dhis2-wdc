(ns dhis2-wdc.core
  (:require-macros [cljs.core.async.macros :as async])
  (:require [dhis2-wdc.wdc :as wdc]
            [reagent.core :as r]
            [cljs-http.client :as http]
            [cljs.core.async :as async]))

(enable-console-print!)
(declare render)

(defn wrap-baseurl [baseurl [endpoint opts]]
  [(str baseurl endpoint) opts])

(defn wrap-cors [cors-proxy [url opts]]
  (if cors-proxy
    [(clojure.string/replace-first url "://" (str "://" cors-proxy "/")) opts]
    [url opts]))

(defn wrap-auth [authorization [url opts]]
  [url (merge-with merge opts {:with-credentials? false
                               :headers {"Authorization" authorization}})])

(defn wrap-accept [[url opts]]
  [url (merge-with merge opts {:headers {"Accept" "application/json"}})])

(defn request [endpoint {:keys [baseurl cors-proxy authorization]}]
  (->> [endpoint nil]
      (wrap-baseurl baseurl)
      (wrap-cors cors-proxy)
      (wrap-auth authorization)
      (wrap-accept)
      (apply http/get)))

(def tables
  {"ou"
   {:endpoint
    "/api/26/organisationUnits?fields=id,level,featureType,displayName,coordinates"

    :table-info
    {:alias   "Organisation Units"
     :columns [{:id       "id"
                :dataType "string"}
               {:id       "level"
                :dataType "int"}
               {:id       "displayName"
                :dataType "string"}
               {:id       "featureType"
                :dataType "string"}
               {:id       "lat"
                :dataType "float"}
               {:id       "lon"
                :dataType "float"}]}

    :response->rows
    (fn [response]
      (->> response
           :body
           :organisationUnits
           (map (fn [{:keys [id level featureType displayName coordinates]}]
                  (let [[lat lon] (when (= featureType "POINT") (js->clj (.parse js/JSON coordinates)))]
                    {:id id
                     :level level
                     :featureType featureType
                     :displayName displayName
                     :lat lat
                     :lon lon})))))}})

(defn paginate [endpoint connection out xform]
  (async/go-loop [endpoint endpoint]
      (when-let [response (async/<! (request endpoint connection))]
        (async/>! out (xform response))
        (let [page  (-> response :body :pager :page)
              count (-> response :body :pager :pageCount)
              has-params? (clojure.string/includes? endpoint "?")
              has-page? (clojure.string/includes? endpoint "page=")
              next (cond
                     (>= page count) nil
                     (not has-params?) (str endpoint "?page=" (inc page))
                     (and has-params? has-page?) (clojure.string/replace endpoint (str "page=" page) (str "page=" (inc page)))
                     (and has-params? (not has-page?)) (str endpoint "&page=" (inc page)))]
          (when next
            (recur next))))))

(deftype DHIS2WDC [connection]
  wdc/IWebDataConnector
  (-get-name [this]
    (str "DHIS2 Connection to " (:baseurl @connection)))
  (-get-table-infos [this]
    (map (fn [[id table]] (assoc (:table-info table) :id id)) tables))
  (-get-standard-connections [this]
    [])
  (-get-rows! [this rows-chan table-info inc-val]
    (when-let [{:keys [endpoint response->rows]} (get tables (:id table-info))]
      (paginate endpoint @connection rows-chan response->rows)))
  (-shutdown [this]
    @connection)
  (-init [this phase state]
    (swap! connection merge state)
    (when (= "interactive" phase)
      (render))))

(defn make-state []
  {:baseurl "https://play.dhis2.org/dev"
   :cors-proxy "cors-anywhere.herokuapp.com"
   :authorization "Basic YWRtaW46ZGlzdHJpY3Q="})

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


