using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Configuration;
using com.bitscopic.downstream.domain.svc;
using com.bitscopic.downstream.net.http;
using gov.va.medora.utils;
using com.bitscopic.downstream.dao.file;

namespace DownstreamDashboard2.domain
{
    public class CachedDashboardObjects
    {
        static DateTime _lastRefreshed = new DateTime(1900, 1, 1);
        static TimeSpan _refreshPeriod = new TimeSpan(0, 15, 0);
        static TaggedExtractorConfigArrays _configDict;
        static ExtractorArray _extractors;
        static String _timeToNextRun;
        static String _lastRunCompletedTime;
        static Dictionary<String, EtlDownstreamStageTO> _etlStages;

        public CachedDashboardObjects()
        {
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings["RefreshObjectCache"]))
            {
                TimeSpan.TryParse(ConfigurationManager.AppSettings["RefreshObjectCache"], out _refreshPeriod);
            }
        }

        public void forceRefresh()
        {
            _lastRefreshed = new DateTime(1900, 1, 1);
            refresh();
        }

        public Dictionary<String, EtlDownstreamStageTO> getEtlStages()
        {
            if (refreshExpired())
            {
                refresh();
            }
            return _etlStages;
        }

        public String getTimeToNextRun()
        {
            if (refreshExpired())
            {
                refresh();
            }
            return _timeToNextRun;
        }

        public String getLastRunCompletedTime()
        {
            if (refreshExpired())
            {
                refresh();
            }
            return _lastRunCompletedTime;
        }

        public ExtractorArray getExtractors()
        {
            if (refreshExpired())
            {
                refresh();
            }
            return _extractors;
        }

        public TaggedExtractorConfigArrays getConfigs()
        {
            if (refreshExpired())
            {
                refresh();
            }
            return _configDict;
        }

        bool refreshExpired()
        {
            if (DateTime.Now.Subtract(_lastRefreshed).CompareTo(_refreshPeriod) > 0)
            {
                return true;
            }
            return false;
        }

        public ReportTO getReport(String siteId, String vistaFile, String batchId)
        {
            if (String.Equals(ConfigurationManager.AppSettings["DemoMode"], "true", StringComparison.CurrentCultureIgnoreCase))
            {
                return new ReportTO() { text = "You are running in demo mode! This is just some boilerplate text. In production, a call will be invoked to the Dashboard web service to fetch the report dynamically" };
            }

            return JsonUtils.Deserialize<ReportTO>(
                new HttpClient(new Uri(ConfigurationManager.AppSettings["DashboardWebServiceURL"])).makeRequest(String.Format("report/{0}/{1}/{2}", siteId, vistaFile, batchId)));
        }

        void refresh()
        {
            _lastRefreshed = DateTime.Now;

            if (String.Equals(ConfigurationManager.AppSettings["DemoMode"], "true", StringComparison.CurrentCultureIgnoreCase))
            {
                refreshFromCachedFiles();
                return;
            }

            _timeToNextRun = (JsonUtils.Deserialize<TextTO>(
                new HttpClient(new Uri(ConfigurationManager.AppSettings["DashboardWebServiceURL"])).makeRequest("nextRunTime"))).text;
            _lastRunCompletedTime = JsonUtils.Deserialize<KeyValuePair<String, DateTime>>(((JsonUtils.Deserialize<TextTO>(
                new HttpClient(new Uri(ConfigurationManager.AppSettings["DashboardWebServiceURL"])).makeRequest("lastRunCompletedTime"))).text)).Value.ToString();
            _configDict = JsonUtils.Deserialize<TaggedExtractorConfigArrays>(
                new HttpClient(new Uri(ConfigurationManager.AppSettings["DashboardWebServiceURL"])).makeRequest("activeJobs"));
            _extractors = JsonUtils.Deserialize<ExtractorArray>(
                new HttpClient(new Uri(ConfigurationManager.AppSettings["DashboardWebServiceURL"])).makeRequest("extractors"));
            if (_configDict != null && _configDict.count > 0)
            {
                bool foundOne = false; // since the findBatchId function below throws an exception if the configs are all empty, we need to check it first since it will be empty between runs
                foreach (TaggedExtractorConfigArray teca in _configDict.values)
                {
                    if (teca != null && teca.count > 0 && teca.value != null && teca.value.Length > 0)
                    {
                        foundOne = true;
                        break;
                    }
                }
                if (!foundOne)
                {
                    return;
                }
                String batchId = findBatchId(_configDict);
                _etlStages = convertStageArrayToDict(JsonUtils.Deserialize<EtlDownstreamStageArray>(
                    new HttpClient(new Uri(ConfigurationManager.AppSettings["DashboardWebServiceURL"])).makeRequest("etlStages/" + batchId)));
            }
        }

        private string findBatchId(TaggedExtractorConfigArrays _configDict)
        {
            // use these three lines while debugging - DON'T FORGET TO COMMENT THEM OUT FOR DEPLOYMENT!!!
            //EtlDownstreamStageArray temp = JsonUtils.Deserialize<EtlDownstreamStageArray>(new FileDao(false).readFile("C:\\inetpub\\wwwroot\\dashboard2\\resources\\data\\etlDowntreamStages.dat"));
            //_etlStages = convertStageArrayToDict(temp);
            //return _etlStages.First().Value.mapItem.downstreamBatchId;

            for (int i = 0; i < _configDict.values.Length; i++)
            {
                if (_configDict.values[i] != null && _configDict.values[i].value != null && _configDict.values[i].value.Length > 0)
                {
                    return _configDict.values[i].value[0].batchId;
                }
            }
            throw new ApplicationException("Couldn't find any extractor configurations to build ETL GUI queries!");
        }

        Dictionary<String, EtlDownstreamStageTO> convertStageArrayToDict(EtlDownstreamStageArray ary)
        {
            Dictionary<String, EtlDownstreamStageTO> result = new Dictionary<string, EtlDownstreamStageTO>();
            for (int i = 0; i < ary.count; i++)
            {
                if (!result.ContainsKey(ary.stages[i].mapItem.siteId))
                {
                    result.Add(ary.stages[i].mapItem.siteId, ary.stages[i]);
                }
            }
            return result;
        }

        void refreshFromCachedFiles()
        {
            _timeToNextRun = DateTime.Now.AddDays(1).ToString(); // new DateTime(2014, 1, 1).ToString();
            _lastRunCompletedTime = DateTime.Now.AddDays(-1).ToString();
            _configDict = JsonUtils.Deserialize<TaggedExtractorConfigArrays>(new FileDao(false).readFile("C:\\inetpub\\wwwroot\\dashboard2\\resources\\data\\activeJobsJson.dat"));
            _extractors = JsonUtils.Deserialize<ExtractorArray>(new FileDao(false).readFile("C:\\inetpub\\wwwroot\\dashboard2\\resources\\data\\extractorsJson.dat"));
            EtlDownstreamStageArray temp = JsonUtils.Deserialize<EtlDownstreamStageArray>(new FileDao(false).readFile("C:\\inetpub\\wwwroot\\dashboard2\\resources\\data\\etlDowntreamStages.dat"));
            _etlStages = convertStageArrayToDict(temp);
        }
    }
}