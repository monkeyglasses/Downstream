using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using com.bitscopic.downstream;
using com.bitscopic.downstream.net;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.net.http;
using gov.va.medora.utils;
using com.bitscopic.downstream.domain.svc;
using DownstreamDashboard2.utils;
using System.Data;
using com.bitscopic.downstream.dao.sql;
using System.Configuration;
using com.bitscopic.downstream.domain.reporting;
using DownstreamDashboard2.domain;

namespace DownstreamDashboard2
{
    public partial class _Default : System.Web.UI.Page
    {
        CachedDashboardObjects _cache;

        protected void Page_Load(object sender, EventArgs e)
        {
            setNotification("");

            (Page.Master.FindControl("linkMainMenuOverview") as HyperLink).CssClass = "nav-text btn active";
            (Page.Master.FindControl("linkMainMenuDetails") as HyperLink).CssClass = "nav-text btn";
            (Page.Master.FindControl("linkMainMenuReports") as HyperLink).CssClass = "nav-text btn";

            if (Request.QueryString != null && Request.QueryString.Count > 0)
            {
                if (Request.QueryString.ToString().Contains("getLog"))
                {
                    if (!String.IsNullOrEmpty(Request.QueryString["site"]) &&
                        !String.IsNullOrEmpty(Request.QueryString["vistaFile"]) &&
                        !String.IsNullOrEmpty(Request.QueryString["batchId"]))
                    {
                        downloadLog(Request.QueryString["site"], Request.QueryString["vistaFile"], Request.QueryString["batchId"]);
                        return;
                    }
                }
            }

            if (Session["MySession"] == null)
            {
                Session["MySession"] = new CachedDashboardObjects();
            }

            _cache = (CachedDashboardObjects)Session["MySession"];

            try
            {
                labelNextRunTime.Text = _cache.getTimeToNextRun();
                labelLastRunCompleted.Text = _cache.getLastRunCompletedTime();

                TaggedExtractorConfigArrays tecas = _cache.getConfigs();
                ExtractorConfigurationTO[] active = getConfigByKey("Active", tecas);
                ExtractorConfigurationTO[] queued = getConfigByKey("Queued", tecas);
                ExtractorConfigurationTO[] completed = getConfigByKey("Completed", tecas);
                ExtractorConfigurationTO[] errored = getConfigByKey("Errored", tecas);

                if (active.Length == 0 && queued.Length == 0 && completed.Length == 0 && errored.Length == 0)
                {
                    setNotification("Downstream is between runs. Please see the next/last run time for information regarding upcoming extraction schedules");
                    return;
                }

                Dictionary<String, EtlDownstreamStageTO> stages = _cache.getEtlStages();

                labelActiveExtractionsCount.Text = active.Length.ToString();
                labelQueuedExtractionsCount.Text = queued.Length.ToString();
                labelCompletedExtractionsCount.Text = completed.Length.ToString();
                labelFailedExtractionsCount.Text = errored.Length.ToString();

                labelActiveEtlCount.Text = getEtlCount(stages, "Active");
                labelCompletedEtlCount.Text = getEtlCount(stages, "Completed");
                labelFailedEtlCount.Text = getEtlCount(stages, "Errored");

                bindFailedExtractionsDataGrid(errored.ToList());
                bindCompletedExtractionsDataGrid(completed.ToList());
                bindInProgressExtractionsDataGrid(active.ToList());
                bindPrioritizedRepeater();
            }
            catch (Exception exc)
            {
                setNotification(exc.Message);
            }
        }

        void setNotification(String message)
        {
            ((Label)Page.Master.FindControl("labelNotifications")).Text = message;
        }

        String getEtlCount(Dictionary<String, EtlDownstreamStageTO> allStages, String type)
        {
            if (allStages == null || allStages.Count == 0)
            {
                return "0";
            }
            int counter = 0;
            foreach (String key in allStages.Keys)
            {
                EtlBatchStage stage = (EtlBatchStage)Enum.Parse(typeof(EtlBatchStage), allStages[key].stage);

                if (String.Equals(type, "Completed"))
                {
                    if (stage >= EtlBatchStage.BCMA_DATA_MART)
                    {
                        counter++;
                    }
                }
                else if (String.Equals(type, "Active"))
                {
                    if ((stage > 0 && stage < EtlBatchStage.COMPLETED_WORKFLOW)) // TODO - doesn't appear there is an "errored" batch stage... need to consider??
                    {
                        counter++;
                    }
                }
                else if (String.Equals(type, "Errored"))
                {
                    // just a placeholder - same comment as above: TODO - doesn't appear there is an "errored" batch stage... need to consider??
                }
            }
            return counter.ToString();
        }

        private void downloadLog(string siteId, string vistaFile, string batchId)
        {
            try
            {
                ReportTO report = new CachedDashboardObjects().getReport(siteId, vistaFile, batchId);

                Response.ContentType = "text/html";
                Response.AddHeader("Content-Disposition", "attachment; filename=\"log.txt\"");
                Response.AddHeader("Content-Length", report.text.Length.ToString());
                Response.Write(report.text);
                Response.Flush();
                Response.Close();
                Response.End();
            }
            catch (Exception exc)
            {
                setNotification(exc.Message);
            }
        }

        ExtractorConfigurationTO[] getConfigByKey(String key, TaggedExtractorConfigArrays tecas)
        {
            for (int i = 0; i < tecas.values.Length; i++)
            {
                if (String.Equals(tecas.values[i].key, key, StringComparison.CurrentCultureIgnoreCase))
                {
                    return tecas.values[i].value;
                }
            }
            return new ExtractorConfigurationTO[0];
        }

        void bindCompletedExtractionsDataGrid(IList<ExtractorConfigurationTO> configs)
        {
            DataTable completedTable = new DataBindingUtils().getTableFromList(configs);

            datagridCompletedExtractions.DataSource = completedTable;
            datagridCompletedExtractions.DataBind();
        }

        void bindInProgressExtractionsDataGrid(IList<ExtractorConfigurationTO> configs)
        {
            DataTable inProgressTable = new DataBindingUtils().getTableFromList(configs);

            datagridInProgressExtractions.DataSource = inProgressTable;
            datagridInProgressExtractions.DataBind();
        }


        void bindFailedExtractionsDataGrid(IList<ExtractorConfigurationTO> configs)
        {
            DataTable failedTable = new DataBindingUtils().getTableFromList(configs);

            datagridFailedExtractions.DataSource = failedTable;
            datagridFailedExtractions.DataBind();
        }

        void bindPrioritizedRepeater()
        {
            return; // prioritization not functional right now so don't show in GUI
            DataTable prioritized = getMockPrioritizedDataTable();

            if (prioritized == null || prioritized.Rows.Count == 0)
            {
                labelPrioritizedExtractions.Text = "No prioritized extractions";
                return;
            }

            labelPrioritizedExtractions.Text = "";
            panelPrioritizedExtractions.Visible = true;

            repeaterPrioritizedExtractions.DataSource = getMockPrioritizedDataTable();
            repeaterPrioritizedExtractions.DataBind();
        }

        DataTable getMockPrioritizedDataTable()
        {
            DataTable mock = new DataTable();
            mock.Columns.Add("SiteName");
            mock.Columns.Add("SiteStatusMarkup");
            mock.Columns.Add("ExtractionMode");
            mock.Columns.Add("LastUpdated");

            mock.Rows.Add(new object[] { "Palo Alto", "<span class='site-status completed'></span>", "Inc", DateTime.Now.ToString() });
            mock.Rows.Add(new object[] { "West L.A.", "<span class='site-status in-progress'></span>", "Reb", DateTime.Now.ToString() });
            mock.Rows.Add(new object[] { "San Francisco", "<span class='site-status failed'></span>", "Inc", DateTime.Now.ToString() });
            return mock;
        }
    }
}
