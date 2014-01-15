using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using com.bitscopic.downstream.config;
using com.bitscopic.downstream.dao.sql;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.svc;
using com.bitscopic.downstream.net;
using System.Configuration;
using com.bitscopic.downstream.utils;
using com.bitscopic.downstream.domain.reporting;

namespace com.bitscopic.downstream.dao.downstream
{
    public class OrchestratorDao
    {
        String _orchestratorHostName;
        Int32 _orchestratorPort;

        public OrchestratorDao()
        {
            _orchestratorHostName = ConfigurationManager.AppSettings[AppConfigSettingsConstants.OrchestratorHostName];
            _orchestratorPort = Convert.ToInt32(ConfigurationManager.AppSettings[AppConfigSettingsConstants.OrchestratorListeningPort]);
        }

        public OrchestratorDao(String hostname, String port)
        {
            _orchestratorHostName = hostname;
            _orchestratorPort = Convert.ToInt32(port);
        }

        public ReportTO getExtractorReport(String siteId, String vistaFile, String batchId)
        {
            ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider],
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString]));
            return new ReportTO(sqlDao.getExtractorReport(siteId, vistaFile, batchId));
            //return sqlDao.getExtractorReport(siteId, vistaFile, batchId).InfoMessages.First();
        }

        public EtlDownstreamStageArray getEtlStagesBySite(String downstreamBatchId)
        {
            ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider],
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString]));
            return new EtlDownstreamStageArray(sqlDao.getEtlStagesForDownstreamBatch(downstreamBatchId));
        }

        public TaggedExtractorConfigArrays getActiveJobs()
        {
            Client c = new Client(_orchestratorHostName, _orchestratorPort);
            String jsonString = c.sendGetWorkListsRequest();
            return gov.va.medora.utils.JsonUtils.Deserialize<TaggedExtractorConfigArrays>(jsonString);
        }

        public IList<Extractor> getExtractors()
        {
            Client c = new Client(_orchestratorHostName, _orchestratorPort);
            return c.sendGetExtractorsRequest();
        }

        public String getNextRunTime()
        {
            Client c = new Client(_orchestratorHostName, _orchestratorPort);
            MessageTO response = c.submitRequest(new MessageTO() { MessageType = MessageTypeTO.TimeToNextRunRequest });
            return response.Message;
        }

        // TODO - allow "*" wildcard for all sites, implement logging if package name doesn't exist or if package mapping file specifies files that aren't added?
        public void prioritizeConfigs(String sites, String packageName)
        {
            PackageTranslator pt = new PackageTranslator();
            IList<String> fileNames = pt.getFilesInPackage(packageName);

            IList<String> siteCodes = sites.Split(new char[] { ';' }).ToList();

            ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider], 
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString]));
            IList<ExtractorConfiguration> configsFromDb = sqlDao.getActiveExtractorConfigurations();

            IList<ExtractorConfiguration> configsToPrioritize = new List<ExtractorConfiguration>();

            foreach (String fileName in fileNames)
            {
                foreach (ExtractorConfiguration ec in configsFromDb)
                {
                    if (String.Equals(fileName, ec.QueryConfigurations.RootNode.Value.File))
                    {
                        foreach (String site in siteCodes)
                        {
                            ExtractorConfiguration newConfig = ec.Clone(); // need to clone because we're giving each config a unique site code!
                            newConfig.SiteCode = site;
                            configsToPrioritize.Add(newConfig);
                        }
                    }
                }
            }

            // finally ready to send to orchestrator!
            prioritizeConfigs(configsToPrioritize);
        }

        public void prioritizeConfigs(IList<ExtractorConfiguration> configs)
        {
            Client c = new Client(_orchestratorHostName, _orchestratorPort);
            c.sendPrioritizationRequest(configs);
        }


        internal String getLastRunCompletedTime()
        {
            Client c = new Client(_orchestratorHostName, _orchestratorPort);
            MessageTO response = c.submitRequest(new MessageTO() { MessageType = MessageTypeTO.LastRunInfoRequest });
            //return gov.va.medora.utils.JsonUtils.Deserialize<KeyValuePair<String, DateTime>>(response.Message);
            return response.Message; // will be JSON serialized KeyValuePair<String, DateTime>
        }
    }
}