using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.service;
using gov.va.medora.mdo;
using com.bitscopic.downstream.domain.svc;
using DownstreamDashboard2.domain;

namespace DownstreamDashboard2.utils
{
    public class DataBindingUtils
    {
        static SiteTable _table;

        public DataTable getTableFromList(IList<ExtractorConfigurationTO> configs)
        {
            DataTable table = new DataTable();
            table.Columns.Add("SiteId");
            table.Columns.Add("SiteName");
            table.Columns.Add("VistaFile");
            table.Columns.Add("ExtractionMode");
            table.Columns.Add("BatchId");
            table.Columns.Add("Timestamp");
            foreach (ExtractorConfigurationTO config in configs)
            {
                //table.Rows.Add(new object[] { config.SiteCode, config.QueryConfigurations.RootNode.Value.File, Enum.GetName(typeof(ExtractorMode), config.ExtractMode), config.BatchId, DateTime.Now });
                table.Rows.Add(new object[] { config.siteCode, getSiteName(config.siteCode), config.queryConfigurations.rootNode.value.file, config.extractorMode, config.batchId, DateTime.Now });
            }
            return table;
        }

        /// <summary>
        /// Get site namewith sitecode in format: Ann Arbor (506)
        /// </summary>
        /// <param name="sitecode"></param>
        /// <returns></returns>
        public static String getSiteNameWithSitecode(String sitecode)
        {
            return String.Concat(getSiteName(sitecode), " (", sitecode, ")");
        }

        public static String getSiteName(String sitecode)
        {
            if (_table == null)
            {
                _table = new SiteTable("C:\\inetpub\\wwwroot\\dashboard2\\resources\\xml\\VhaSites.xml");
            }

            return _table.getSite(sitecode).Name;
        }

        public static Dictionary<String, String> getSiteIdAndNameDict()
        {
            if (_table == null)
            {
                _table = new SiteTable("C:\\inetpub\\wwwroot\\dashboard2\\resources\\xml\\VhaSites.xml");
            }

            Dictionary<String, String> result = new Dictionary<string, string>();

            for (int i = 0; i < _table.Sites.Count; i++)
            {
                String siteId = ((Site)_table.Sites.GetByIndex(i)).Id;
                String siteName = ((Site)_table.Sites.GetByIndex(i)).Name;
                //DataSource src = ((Site)_table.Sites.GetByIndex(i)).getDataSourceByModality("HIS");
                if (!result.ContainsKey(siteId))
                {
                    result.Add(siteId, siteName);
                }
            }

            return result;
        }

        public static String getProgressBarCssClassMarkupFromEnum(ProgressBarType pbt)
        {
            switch (pbt)
            {
                case ProgressBarType.Completed_Completed:
                    return "<li class='progress-bar c-c'></li>";
                case ProgressBarType.Completed_Failed:
                    return "<li class='progress-bar c-f'></li>";
                case ProgressBarType.Completed_InProgress:
                    return "<li class='progress-bar c-i'></li>";
                case ProgressBarType.Failed_NotStarted:
                    return "<li class='progress-bar f-n'></li>";
                case ProgressBarType.InProgress_NotStarted:
                    return "<li class='progress-bar i-n'></li>";
                case ProgressBarType.NotStarted_NotStarted:
                    return "<li class='progress-bar n-n'></li>";
                default:
                    return String.Empty;
            }
        }

        public static String getStatusCssClassMarkupFromEnum(ProgressBarType pbt)
        {
            switch (pbt)
            {
                case ProgressBarType.Completed_Completed:
                    return "completed";
                case ProgressBarType.Completed_Failed:
                    return "failed";
                case ProgressBarType.Completed_InProgress:
                    return "inProgress";
                case ProgressBarType.Failed_NotStarted:
                    return "failed";
                case ProgressBarType.InProgress_NotStarted:
                    return "inProgress";
                case ProgressBarType.NotStarted_NotStarted:
                    return "notStarted";
                default:
                    return String.Empty;
            }
        }

        public static String getProgressBarTextFromEnum(ProgressBarType pbt)
        {
            switch (pbt)
            {
                case ProgressBarType.Completed_Completed:
                    return "Completed";
                case ProgressBarType.Completed_Failed:
                    return "Failed";
                case ProgressBarType.Completed_InProgress:
                    return "In-Progress";
                case ProgressBarType.Failed_NotStarted:
                    return "Failed";
                case ProgressBarType.InProgress_NotStarted:
                    return "In-Progress";
                case ProgressBarType.NotStarted_NotStarted:
                    return "Not Started";
                default:
                    return String.Empty;
            }
        }
    
    }
}