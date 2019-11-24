
from datetime import datetime, timedelta

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

import tools.utils.dfutils as dfutils



import tools.model.model_classes as merm_model
import tools.utils.dfutils as dfutils
import tools.utils.log as log


class UniquePageViewIntegration:

    def __init__(self):
        pass

    def perform(self, package:merm_model.PipelinePackage):
        log.getLogger().info( "Processing Unique Page Views from flat file")
        log.getLogger().info("Corpus size: " + str(len(package.linked_document_list)))
        csv = package.dependencies_dict["env"].config["local_data"]["confluence_uniquepageviews"]
        df = pd.read_csv(csv)



        dfutils.print_vals_in_column(df, "Page")
        if "confluence" in package.any_analysis_dict["provider"]:
            for doc in package.linked_document_list:
                #upv_match = df.loc[df['Page'] == doc.ui]
                #log.getLogger().debug("in ld: " + str(doc.ui))

                df['Page'] = df['Page'].fillna('')
                #upv_match = df[df['Page'].str.match(doc.ui)]
                if doc.provider == "confluence$$":
                    upv_match = df[df['Page'].str.contains(doc.ui, regex=False)]
                    for i in upv_match.index:
                        dfutils.print_vals_in_column(upv_match, "Page", 3)
                        dfutils.print_vals_in_column(upv_match, "Unique Pageviews", 3)
                        webui = upv_match.at[i, 'Page']

                        view_count =  upv_match.at[i, 'Unique Pageviews']
                        log.getLogger().debug("webui" + webui + "view_count " + view_count)
                        doc.scores["page_views"] = view_count
                        log.getLogger().debug("added score" + str(doc.scores))

        return package




class UniquePageViewIntegrationDynamic:
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

    def __init__(self):
        pass

    def _initialize_analyticsreporting(self, package):
        """Initializes an Analytics Reporting API V4 service object.

        Returns:
          An authorized Analytics Reporting API V4 service object.
        """
        KEY_FILE_LOCATION = package.dependencies_dict["env"].config_a["ga"]["file"]
        credentials = Credentials.from_service_account_file(KEY_FILE_LOCATION, scopes=self.SCOPES)

        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics

    def _get_report(self, analytics, page_size, next_page_token):
        """Queries the Analytics Reporting API V4.

        Args:
          analytics: An authorized Analytics Reporting API V4 service object.
        Returns:
          The Analytics Reporting API V4 response.
        """
        #print("next_page_token: " + next_page_token)
        start = self._date_string(90)
        end = self._date_string(0)

        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': "192066861",
                        'dateRanges': [{'startDate': start, 'endDate': end}],
                        'metrics': [{'expression': 'ga:uniquePageviews'}],
                        'dimensions': [{'name': 'ga:pagePath'}],
                        'pageToken': next_page_token,
                        'pageSize': page_size
                    }]
            }
        ).execute()

    def _doc_uid(self, docid):
        #log.getLogger().info("docid in " + docid)
        last_idx = docid.rfind("/")
        if last_idx < len(docid):
            suffix = docid[last_idx+1:len(docid)]
            #log.getLogger().info("suffix: " + suffix)
            if suffix.isnumeric():
                return suffix
            else:
                return docid[0:last_idx]
        else:
            return docid

    def _date_string(self, days_to_subtract):
        # now = datetime.datetime.now()
        d = datetime.today() - timedelta(days=days_to_subtract)

        return d.strftime("%Y-%m-%d")

    def _print_response(self, data_rows):
        """Parses and prints the Analytics Reporting API V4 response.

        Args:
          response: An Analytics Reporting API V4 response.
        """

        results_dict = {}

        for row in data_rows:
            page = row.get("dimensions").pop()
            upv = row.get("metrics").pop()
            if "dosearchsite.action?" not in page:
                results_dict[page] = upv["values"].pop().replace('\n', '')

        return results_dict

    def perform(self, package:merm_model.PipelinePackage):

        analytics = self._initialize_analyticsreporting(package)
        response = self._get_report(analytics, 1000, "0")
        rows = response.get("reports").pop()
        data_rows = rows.get("data").get("rows")
        while rows.get('nextPageToken') != None:
            response = self._get_report(analytics, 1000, rows.get('nextPageToken'))
            rows = response.get("reports").pop()
            # response = self.getResponse(oResponse, False)
            data_rows.extend(rows.get("data").get("rows"))

        results_dict = self._print_response(data_rows)
        df = pd.DataFrame(list(results_dict.items()), columns=['Page', 'Unique Pageviews'])
        log.getLogger().info("Google Analytics data: " + str(df.shape))
        dfutils.print_vals_in_column(df, "Page", 30)

        valid_hits = {}
        for doc in package.linked_document_list:
            # upv_match = df.loc[df['Page'] == doc.ui]
            # log.getLogger().debug("in ld: " + str(doc.ui))

            df['Page'] = df['Page'].fillna('')
            # upv_match = df[df['Page'].str.match(doc.ui)]
            if doc.provider == "confluence$$":
                doc_id =self._doc_uid(doc.ui)

                #log.getLogger().info("doc_id: " +  doc_id)
                upv_match = df[df['Page'].str.contains(doc_id, regex=False)]

                for i in upv_match.index:
                    page = dfutils.print_vals_in_column(upv_match, "Page", 3)
                    upv = dfutils.print_vals_in_column(upv_match, "Unique Pageviews", 3)
                    webui = upv_match.at[i, 'Page']

                    view_count = upv_match.at[i, 'Unique Pageviews']
                    #log.getLogger().debug("webui" + webui + "view_count " + view_count)
                    doc.scores["page_views"] = view_count
                    valid_hits[webui] = view_count
                    #log.getLogger().debug("added score" + str(doc.scores))


        log.getLogger().info(str(valid_hits))
        log.getLogger().info("GA Valid hit count: " + str(len(valid_hits)))

        return package






