# DHIS2 Tableau Web Data Connector

DHIS2 is the preferred health management information system in 47 countries and 23 organizations across four continents. Tableau is data visualization software that lets you see and understand data in minutes. Web Data Connector allows you to connect web data to Tableau.

## Overview

Tableau is building a Web Data Connector (WDC) to DHIS2, a health management information system that has become exceedingly popular in developing countries over the past few years. DHIS2 is primarily built as a countrywide platform that allows for widespread health data collection, analysis, and visualization. Development agencies and similar NGOs tend to develop a country’s platform, with the data and analysis being provided to the country’s health ministry. NGOs partnered with the country’s health ministry often have access to this data as well.

While DHIS2 is billed as being a comprehensive storage and analysis tool, even the DHIS2 developers support the creation of a Tableau connector to DHIS2 to address significant limitations with reporting and visualizing of data stored in DHIS2 implementations.
The connector will allow users to:
1. Connect to the application using OAuth2
2. Turn user selected data elements into Tableau TDEs using the DHIS2 APIs
3. Interact with the generated TDEs in Tableau Desktop
4. Publish the data source to Tableau Online & Server
5. In Tableau Online & Server successfully execute full refreshes on the data source extract.
This will allow the users to have Tableau Online & Server pull “fresh data daily” from their account.

## Setup

Refer to Tableau Web Data Connector [documentation hub](http://tableau.github.io/webdataconnector/) for WDC specifics.

To get an interactive development environment run:

    lein figwheel

and open your browser at [localhost:3449](http://localhost:3449/).
This will auto compile and send all changes to the browser without the
need to reload. After the compilation process is complete, you will
get a Browser Connected REPL. An easy way to try it is:

    (js/alert "Am I connected?")

and you should see an alert in the browser window.

To clean all compiled files:

    lein clean

To create a production build run:

    lein do clean, cljsbuild once min

And open your browser in `resources/public/index.html`. You will not
get live reloading, nor a REPL. 

## License

Copyright © 2017 Tableau

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
