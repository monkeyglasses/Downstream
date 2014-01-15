using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using com.bitscopic.downstream.dao.downstream;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.svc;
using DownstreamDashboard;
using System.ServiceModel.Web;
using System.Configuration;
using System.IO;

namespace com.bitscopic.downstream
{
    [ServiceBehavior(InstanceContextMode = InstanceContextMode.Single)]
    public class DashboardSvc : IDashboardSvc
    {
        public Stream getEtlStages(String downstreamBatchId)
        {
            try
            {
                EtlDownstreamStageArray result = new OrchestratorDao().getEtlStagesBySite(downstreamBatchId);
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<EtlDownstreamStageArray>(result);
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
            catch (Exception exc)
            {
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<EtlDownstreamStageArray>(new EtlDownstreamStageArray() { fault = new FaultTO(exc) });
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
        }

        public Stream getReport(String siteId, String vistaFile, String batchId)
        {
            try
            {
                ReportTO report = new OrchestratorDao().getExtractorReport(siteId, vistaFile, batchId);
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<ReportTO>(report);
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
            catch (Exception exc)
            {
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<ReportTO>(new ReportTO() { fault = new FaultTO(exc) });
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
        }

        public Stream getLastRunCompletedTime()
        {
            try
            {
                String result = new OrchestratorDao().getLastRunCompletedTime();
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<TextTO>(new TextTO() { text = result });
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
            catch (Exception exc)
            {
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<TextTO>(new TextTO() { fault = new FaultTO(exc) });
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
        }

        public Stream getNextRunTime()
        {
            return getNextRunTimeForOrchestrator(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorHostName],
                    ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorListeningPort]);
        }

        public Stream getNextRunTimeForOrchestrator(String orchestratorHostname, String port)
        {
            try
            {
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(gov.va.medora.utils.JsonUtils.Serialize<TextTO>(
                    new TextTO()
                    {
                        text = new OrchestratorDao(orchestratorHostname, port).getNextRunTime()
                    })));
            }
            catch (Exception exc)
            {
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(gov.va.medora.utils.JsonUtils.Serialize<TextTO>(
                    new TextTO() { fault = new FaultTO(exc) })));
            }
        }

        public Stream startOnDemandPackage(String sites, String packageName)
        {
            BoolTO response = new BoolTO() { tf = true };

            try
            {
                response.fault = new FaultTO() { message = "Not yet implemented" };
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(gov.va.medora.utils.JsonUtils.Serialize<BoolTO>(response)));
                
                new OrchestratorDao().prioritizeConfigs(sites, packageName);
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(gov.va.medora.utils.JsonUtils.Serialize<BoolTO>(response)));
            }
            catch (Exception exc)
            {
                response.fault = new FaultTO(exc);

                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(gov.va.medora.utils.JsonUtils.Serialize<BoolTO>(response)));
            }
        }

        public Stream getExtractors()
        {
            try
            {
                IList<Extractor> extractors = new OrchestratorDao().getExtractors();
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
                    gov.va.medora.utils.JsonUtils.Serialize<ExtractorArray>(new ExtractorArray(extractors))));
            }
            catch (Exception exc)
            {
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
                    gov.va.medora.utils.JsonUtils.Serialize<ExtractorArray>(new ExtractorArray() { fault = new FaultTO(exc) })));
            }
        }

        public Stream getExtractorsLite()
        {
            try
            {
                IList<Extractor> extractors = new OrchestratorDao().getExtractors();
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<ExtractorArrayLite>(new ExtractorArrayLite(extractors));
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
            catch (Exception exc)
            {
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<ExtractorArrayLite>(new ExtractorArrayLite() { fault = new FaultTO(exc) });
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
        }

        public Stream getActiveJobs()
        {
            try
            {
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
                    gov.va.medora.utils.JsonUtils.Serialize<TaggedExtractorConfigArrays>(new OrchestratorDao().getActiveJobs())));
            }
            catch (Exception exc)
            {
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
                     gov.va.medora.utils.JsonUtils.Serialize<TaggedExtractorConfigArrays>(new TaggedExtractorConfigArrays() { fault = new FaultTO(exc) })));
            }
        }

        public Stream getActiveJobsLite()
        {
            try
            {
                String jsonString = gov.va.medora.utils.JsonUtils.Serialize<TaggedExtractorConfigArraysLite>(new TaggedExtractorConfigArraysLite(new OrchestratorDao().getActiveJobs()));
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonString));
            }
            catch (Exception exc)
            {
                return new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
                    gov.va.medora.utils.JsonUtils.Serialize<TaggedExtractorConfigArraysLite>(new TaggedExtractorConfigArraysLite() { fault = new FaultTO(exc) })));
            }
        }

        public String helloWorld(String name)
        {
            if (String.IsNullOrEmpty(name))
            {
                return "Say your name!";
            }
            if (String.Equals(name, "farshid", StringComparison.CurrentCultureIgnoreCase))
            {
                return "I'm not talking to you, Farshid...";
            }
            return String.Concat("Hello ", name, "!!!");
        }
    }

    [ServiceContract(Namespace = "http://downstream.bitscopic.com/DashboardSvc")]
    public interface IDashboardSvc
    {
        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "etlStages/{batchId}")]
        Stream getEtlStages(String batchId);

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "report/{siteId}/{vistaFile}/{batchId}")]
        Stream getReport(String siteId, String vistaFile, String batchId);

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "lastRunCompletedTime")]
        Stream getLastRunCompletedTime();

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "nextRunTime")]
        Stream getNextRunTime();

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "nextRunTime?orchestrator={orchestratorHostname}&port={port}")]
        Stream getNextRunTimeForOrchestrator(String orchestratorHostname, String port);

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "startOnDemandPackage?packageName={packageName}&sites={sites}")]
        Stream startOnDemandPackage(String packageName, String sites);

        /// <summary>
        /// Our interface is using a string because sometimes the responses are too large for .NET to serialize and nothing is returned
        /// to the client. Utilizing our own json serializer (via MDO DLL) provides much more robust messaging outcomes
        /// </summary>
        /// <returns>JSON string of TaggedExtractorConfigArrays</returns>
        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "activeJobs")]
        Stream getActiveJobs();

        /// <summary>
        /// Our interface is using a string because sometimes the responses are too large for .NET to serialize and nothing is returned
        /// to the client. Utilizing our own json serializer (via MDO DLL) provides much more robust messaging outcomes.
        /// 
        /// This API utilizes a smaller footprint TO useful when all that is needed is the job lists where each configuration contains 
        /// only the Vista file and site code
        /// </summary>
        /// <returns>JSON string of TaggedExtractorConfigArraysLite</returns>
        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "activeJobsLite")]
        Stream getActiveJobsLite();

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "extractors")]
        Stream getExtractors();

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "extractorsLite")]
        Stream getExtractorsLite();

        [OperationContract]
        [WebInvoke(Method = "GET", ResponseFormat = WebMessageFormat.Json, UriTemplate = "hello/{name}")]
        String helloWorld(String name);
    }

}
