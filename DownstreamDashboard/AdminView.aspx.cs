using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using com.bitscopic.downstream.net;
using com.bitscopic.downstream.domain;
using System.Configuration;
using System.Data;
using com.bitscopic.downstream.domain.svc;
using gov.va.medora.utils;

namespace DownstreamDashboard
{
    public partial class AdminView : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (Request.QueryString["show"] == "Overview")
            {
                // show overview
            }
            else if (Request.QueryString["show"] == "Reports")
            {
                // show reports
            }
            string orchestratorHostName = ConfigurationManager.AppSettings[com.bitscopic.downstream.config.AppConfigSettingsConstants.OrchestratorHostName];

            // allow user to dynamically change via request URL
            if (Request.QueryString != null && Request.QueryString.Count > 0 && !String.IsNullOrEmpty(Request.QueryString["orchestrator"]))
            {
                orchestratorHostName = Request.QueryString["orchestrator"];
            }

            Client dashboardClient = new Client();
            try
            {
                Int32 orchestratorListeningPort = Convert.ToInt32(ConfigurationManager.AppSettings[com.bitscopic.downstream.config.AppConfigSettingsConstants.OrchestratorListeningPort]);
                dashboardClient.connect(orchestratorHostName, orchestratorListeningPort);

                String jsonString = dashboardClient.sendGetWorkListsRequest();
                TaggedExtractorConfigArrays deserialized = JsonUtils.Deserialize<TaggedExtractorConfigArrays>(jsonString);
                Dictionary<string, IList<ExtractorConfiguration>> worklists = deserialized.convertToDictionary();

                IList<Extractor> extractors = dashboardClient.sendGetExtractorsRequest();

                if (extractors != null && extractors.Count > 0)
                {
                    labelActiveCount.Text = extractors.Count.ToString();
                    datagridActive.DataSource = getActiveDataTable(extractors);
                    datagridActive.DataBind();
                }
                else
                {
                    labelActiveCount.Text = "No active jobs!";
                    datagridActive.Visible = false;
                }

                bindDataGrid("Queued", worklists, datagridQueued);
                bindDataGrid("Completed", worklists, datagridCompleted);
                bindDataGrid("Errored", worklists, datagridErrored);
            }
            catch (Exception)
            {
                labelMessage.Text = "The orchestrator does not appear to be running!";
            }            
        }

        void bindDataGrid(string type, Dictionary<string, IList<ExtractorConfiguration>> source, DataGrid dg)
        {
            // reset these
            datagridErrored.Visible = datagridCompleted.Visible = datagridQueued.Visible = true;
            if (source == null || source.Count == 0 || !source.ContainsKey(type) || source[type] == null || source[type].Count == 0)
            {
                if (type == "Queued")
                {
                    labelQueuedCount.Text = "No queued jobs!";
                    datagridQueued.Visible = false;
                }
                else if (type == "Completed")
                {
                    labelCompletedCount.Text = "No completed jobs!";
                    datagridCompleted.Visible = false;
                }
                else if (type == "Errored")
                {
                    labelErroredCount.Text = "No errored jobs!";
                    datagridErrored.Visible = false;
                }
                return;
            }

            if (type == "Queued")
            {
                labelQueuedCount.Text = source[type].Count.ToString();
            }
            else if (type == "Completed")
            {
                labelCompletedCount.Text = source[type].Count.ToString();
            }
            else if (type == "Errored")
            {
                labelErroredCount.Text = source[type].Count.ToString();
            }

            DataTable dt = new DataTable();
            Dictionary<string, string> vistaFiles = new Dictionary<string, string>();
            foreach (ExtractorConfiguration config in source[type])
            {
                if (!vistaFiles.ContainsKey(config.QueryConfigurations.RootNode.Value.File))
                {
                    vistaFiles.Add(config.QueryConfigurations.RootNode.Value.File, "");
                }
                if (String.IsNullOrEmpty(vistaFiles[config.QueryConfigurations.RootNode.Value.File]))
                {
                    vistaFiles[config.QueryConfigurations.RootNode.Value.File] = config.SiteCode;
                }
                else
                {
                    vistaFiles[config.QueryConfigurations.RootNode.Value.File] += (", " + config.SiteCode);
                }
            }
            if (source.ContainsKey(type) && source[type] != null && source[type].Count > 0)
            {
                dg.DataSource = vistaFiles;
                dg.DataBind();
            }
        }

        DataTable getActiveDataTable(IList<Extractor> extractors)
        {
            DataTable result = new DataTable();
            result.Columns.Add(new DataColumn("SITECODE"));
            result.Columns.Add(new DataColumn("VISTAFILE"));
            result.Columns.Add(new DataColumn("HOSTNAME"));
            result.Columns.Add(new DataColumn("TIMESTAMP"));

            foreach (Extractor e in extractors)
            {
                object[] rowVals = new object[4];
                rowVals[0] = e.SiteCode;
                rowVals[1] = e.VistaFile;
                rowVals[2] = e.HostName + ":" + e.ListeningPort;
                rowVals[3] = e.Timestamp;
                result.Rows.Add(rowVals);
            }

            return result;
        }

    }
}
