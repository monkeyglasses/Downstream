using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using com.bitscopic.downstream.domain.svc;
using gov.va.medora.utils;
using System.Data;
using DownstreamDashboard2.utils;
using DownstreamDashboard2.domain;
using com.bitscopic.downstream.domain;

namespace DownstreamDashboard2
{
    public partial class Details : System.Web.UI.Page
    {
        CachedDashboardObjects _cache; 

        protected void Page_Load(object sender, EventArgs e)
        {
            setNotification("");

            (Page.Master.FindControl("linkMainMenuDetails") as HyperLink).CssClass = "nav-text btn active";
            (Page.Master.FindControl("linkMainMenuOverview") as HyperLink).CssClass = "nav-text btn";
            (Page.Master.FindControl("linkMainMenuReports") as HyperLink).CssClass = "nav-text btn";

            if (Session["MySession"] == null)
            {
                Session["MySession"] = new CachedDashboardObjects();
            }

            _cache = (CachedDashboardObjects)Session["MySession"];

            try
            {
                TaggedExtractorConfigArrays tecas = _cache.getConfigs();
                ExtractorConfigurationTO[] active = getConfigByKey("Active", tecas);
                ExtractorConfigurationTO[] queued = getConfigByKey("Queued", tecas);
                ExtractorConfigurationTO[] completed = getConfigByKey("Completed", tecas);
                ExtractorConfigurationTO[] errored = getConfigByKey("Errored", tecas);

                Dictionary<String, EtlDownstreamStageTO> stageBySite = _cache.getEtlStages();

                labelRunningCount.Text = active.Length.ToString();
                labelFailureCount.Text = errored.Length.ToString();

                bindRepeater(getBindableTable(tecas, stageBySite));
            }
            catch (Exception exc)
            {
                ((Label)Page.Master.FindControl("labelNotifications")).Text = exc.Message;
            }
        }

        void setNotification(String message)
        {
            ((Label)Page.Master.FindControl("labelNotifications")).Text = message;
        }

        ExtractorConfigurationTO[] getConfigByKey(String key, TaggedExtractorConfigArrays teca)
        {
            for (int i = 0; i < teca.values.Length; i++)
            {
                if (String.Equals(teca.values[i].key, key, StringComparison.CurrentCultureIgnoreCase))
                {
                    return teca.values[i].value;
                }
            }
            return new ExtractorConfigurationTO[0];
        }

        #region COMMENT_OUT
        void bindRepeater(DataTable table)
        {
            repeaterStatus.DataSource = table;
            repeaterStatus.DataBind();
        }

        DataTable getBindableTable(TaggedExtractorConfigArrays tecas, Dictionary<String, EtlDownstreamStageTO> etlStageBySite)
        {
            Dictionary<String, String> siteIdsAndNames = DataBindingUtils.getSiteIdAndNameDict();
            Dictionary<String, IList<ExtractorConfigurationTO>> active = getConfigsByKeyAndSite("Active", tecas);
            Dictionary<String, IList<ExtractorConfigurationTO>> queued = getConfigsByKeyAndSite("Queued", tecas);
            Dictionary<String, IList<ExtractorConfigurationTO>> errored = getConfigsByKeyAndSite("Errored", tecas);
            Dictionary<String, IList<ExtractorConfigurationTO>> completed = getConfigsByKeyAndSite("Completed", tecas);

            DataTable mock = new DataTable();
            mock.Columns.Add("site");
            mock.Columns.Add("progressBarMarkup");
            mock.Columns.Add("status");
            mock.Columns.Add("statusMarkup");
            mock.Columns.Add("adtStatus");
            mock.Columns.Add("adtFinishedTimestamp");
            mock.Columns.Add("labStatus");
            mock.Columns.Add("labFinishedTimestamp");
            mock.Columns.Add("vitalsStatus");
            mock.Columns.Add("vitalsFinishedTimestamp");
            mock.Columns.Add("bcmaStatus");
            mock.Columns.Add("bcmaFinishedTimestamp");
            mock.Columns.Add("pharmaStatus");
            mock.Columns.Add("pharmaFinishedTimestamp");

            for (int i = 0; i < siteIdsAndNames.Count; i++)
            {
                String currentSite = siteIdsAndNames.ElementAt(i).Key;

                String dateTimeFormatString = "M/d h:mm tt";

                // if any config has been marked errored for this site - show failure
                if (errored.ContainsKey(currentSite) && errored[currentSite] != null && errored[currentSite].Count > 0)
                {
                    String controlsText = DataBindingUtils.getProgressBarTextFromEnum(ProgressBarType.Failed_NotStarted);
                    String statusMarkup = DataBindingUtils.getStatusCssClassMarkupFromEnum(ProgressBarType.Failed_NotStarted);
                    mock.Rows.Add(new object[] { siteIdsAndNames.ElementAt(i).Value, DataBindingUtils.getProgressBarCssClassMarkupFromEnum(ProgressBarType.Failed_NotStarted), controlsText, statusMarkup,
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // ADT
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // labs
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // vitals
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // BCMA
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // pharma
                    });

                    continue;
                }

                // if all jobs for a site are in queued and no other collections contain site ID, then mark as not started
                if (!errored.ContainsKey(currentSite) && !active.ContainsKey(currentSite) && !completed.ContainsKey(currentSite) && queued.ContainsKey(currentSite))
                {
                    String controlsText = DataBindingUtils.getProgressBarTextFromEnum(ProgressBarType.NotStarted_NotStarted);
                    String statusMarkup = DataBindingUtils.getStatusCssClassMarkupFromEnum(ProgressBarType.NotStarted_NotStarted);
                    mock.Rows.Add(new object[] { siteIdsAndNames.ElementAt(i).Value, DataBindingUtils.getProgressBarCssClassMarkupFromEnum(ProgressBarType.NotStarted_NotStarted), controlsText, statusMarkup,
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // ADT
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // labs
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // vitals
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // BCMA
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // pharma
                    });

                    continue;
                }

                // if any config for this site is running or queued - show in progress
                if ((queued.ContainsKey(currentSite) && queued[currentSite] != null && queued[currentSite].Count > 0) ||
                    (active.ContainsKey(currentSite) && active[currentSite] != null && active[currentSite].Count > 0))
                {
                    String controlsText = DataBindingUtils.getProgressBarTextFromEnum(ProgressBarType.InProgress_NotStarted);
                    String statusMarkup = DataBindingUtils.getStatusCssClassMarkupFromEnum(ProgressBarType.InProgress_NotStarted);
                    mock.Rows.Add(new object[] { siteIdsAndNames.ElementAt(i).Value, DataBindingUtils.getProgressBarCssClassMarkupFromEnum(ProgressBarType.InProgress_NotStarted), controlsText, statusMarkup,
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // ADT
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // labs
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // vitals
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // BCMA
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // pharma
                    });

                    continue;
                }

                // if we show at least one job completed for this site and no configs found in active or queued or errored, consider it complete!
                if (completed.ContainsKey(currentSite) && completed[currentSite] != null && completed[currentSite].Count > 0)
                {
                    String controlsText = ""; 
                    String statusMarkup = "";
                    ProgressBarType currentType = 0;

                    if (etlStageBySite.ContainsKey(currentSite))
                    {
                        EtlBatchStage etlStage = (EtlBatchStage)Enum.Parse(typeof(EtlBatchStage), etlStageBySite[currentSite].stage);

                        if (etlStage == EtlBatchStage.COMPLETED_WORKFLOW)
                        {
                            controlsText = DataBindingUtils.getProgressBarTextFromEnum(ProgressBarType.Completed_Completed);
                            statusMarkup = DataBindingUtils.getStatusCssClassMarkupFromEnum(ProgressBarType.Completed_Completed);
                            currentType = ProgressBarType.Completed_Completed;
                        }
                        else if (etlStage == EtlBatchStage.NOT_STARTED)
                        {
                            controlsText = DataBindingUtils.getProgressBarTextFromEnum(ProgressBarType.Completed_InProgress);
                            statusMarkup = DataBindingUtils.getStatusCssClassMarkupFromEnum(ProgressBarType.Completed_InProgress);
                            currentType = ProgressBarType.Completed_InProgress;
                        }
                        else if (etlStage >= EtlBatchStage.START_WORKFLOW)
                        {
                            controlsText = DataBindingUtils.getProgressBarTextFromEnum(ProgressBarType.Completed_InProgress);
                            statusMarkup = DataBindingUtils.getStatusCssClassMarkupFromEnum(ProgressBarType.Completed_InProgress);
                            currentType = ProgressBarType.Completed_InProgress;
                        }
                        // TBD - how do we show failed ETL?????
                    }
                    else // didn't find site ID in ETL stages
                    {
                        controlsText = DataBindingUtils.getProgressBarTextFromEnum(ProgressBarType.Completed_InProgress); // TBD - should there be a Completed_notStarted  status??? seems like there should be...
                        statusMarkup = DataBindingUtils.getStatusCssClassMarkupFromEnum(ProgressBarType.Completed_InProgress);
                        currentType = ProgressBarType.Completed_InProgress;
                    }
                    mock.Rows.Add(new object[] { siteIdsAndNames.ElementAt(i).Value, DataBindingUtils.getProgressBarCssClassMarkupFromEnum(currentType), controlsText, statusMarkup,
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // ADT
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // labs
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // vitals
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // BCMA
                        controlsText, DateTime.Now.ToString(dateTimeFormatString), // pharma
                    });

                    continue;
                }

            }

            return mock;
        }



        Dictionary<String, IList<ExtractorConfigurationTO>> getConfigsByKeyAndSite(String key, TaggedExtractorConfigArrays tecas)
        {
            Dictionary<String, IList<ExtractorConfigurationTO>> result = new Dictionary<string, IList<ExtractorConfigurationTO>>();

            ExtractorConfigurationTO[] configs = getConfigByKey(key, tecas);
            if (configs == null || configs.Length == 0)
            {
                return result;
            }

            for (int i = 0; i < configs.Length; i++)
            {
                if (!result.ContainsKey(configs[i].siteCode))
                {
                    result.Add(configs[i].siteCode, new List<ExtractorConfigurationTO>());
                }

                result[configs[i].siteCode].Add(configs[i]);
            }

            return result;
        }
        #endregion

    }
}