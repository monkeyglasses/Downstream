using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.reporting;
using System.Data;

namespace com.bitscopic.downstream.net
{
    [Serializable]
    public class MessageTO
    {
        /// <summary>
        /// A collection of System.Data.DataTable objects compressed using the GZip class
        /// </summary>
        public Dictionary<string, byte[]> CompressedDataTables { get; set; }
        /// <summary>
        /// The file bytes for the 7zip file containing the extracted data tables
        /// </summary>
        public byte[] ZippedFile { get; set; }
        /// <summary>
        /// Use to send DataTable build from VistaDao
        /// </summary>
        public DataTable VistaData { get; set; }
        /// <summary>
        /// A string message
        /// </summary>
        public string Message { get; set; }
        /// <summary>
        /// A string error
        /// </summary>
        public string Error { get; set; }
        /// <summary>
        /// An extractor configuration object
        /// </summary>
        public ExtractorConfiguration Configuration { get; set; }
        /// <summary>
        /// An extraction job's settings
        /// </summary>
        public Extractor Extractor { get; set; }
        /// <summary>
        /// An extraction job's settings
        /// </summary>
        public IList<Extractor> Extractors { get; set; }
        /// <summary>
        /// The message sender's hostname
        /// </summary>
        public string HostName { get; set; }
        /// <summary>
        /// The message sender's listening port
        /// </summary>
        public int ListeningPort { get; set; }
        /// <summary>
        /// The type of message
        /// </summary>
        public MessageTypeTO MessageType { get; set; }
        /// <summary>
        /// Generic list of extractor configurations
        /// </summary>
        public Dictionary<string, IList<ExtractorConfiguration>> Configurations { get; set; }
        /// <summary>
        /// The report created by an extractor
        /// </summary>
        public ExtractorReport ExtractorReport { get; set; }

        /// <summary>
        /// Build a message with HostName, Listening Port, Message Type, Message and Error properties
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Hostname:Port - " + HostName + ":" + ListeningPort);
            sb.AppendLine("Message Type - " + Enum.GetName(typeof(MessageTypeTO), MessageType));
            sb.AppendLine("Message - " + Message);
            sb.AppendLine("Error - " + Error);
            return sb.ToString();
        }

        public byte[] serialize()
        {
            return System.Text.Encoding.UTF8.GetBytes(gov.va.medora.utils.JsonUtils.Serialize<MessageTO>(this));
        }

        public static MessageTO deserialize(byte[] serializedMessageTO)
        {
            return gov.va.medora.utils.JsonUtils.Deserialize<MessageTO>(System.Text.Encoding.UTF8.GetString(serializedMessageTO));
        }
    }


    [Serializable]
    public enum MessageTypeTO
    {
        ServerHasWorkRequest,
        ServerHasWorkResponse,
        StopServerRequest,
        StopServerResponse,
        NewJobRequest,
        NewJobResponse,
        JobCompletedRequest,
        JobCompletedResponse,
        JobStatusRequest,
        JobStatusResponse,
        JobErrorRequest,
        JobErrorResponse,
        LogErrorRequest,
        TableUploadRequest,
        TableUploadResponse,
        TablesUploadRequest,
        TablesUploadResponse,
        ZipFileUploadRequest,
        ZipFileUploadResponse,
        WorkStacksRequest,
        WorkStacksResponse,
        ExtractorsRequest,
        ExtractorsResponse,
        Error,
        JobPrioritizationRequest,
        JobPrioritizationResponse,
        TimeToNextRunRequest,
        TimeToNextRunResponse,
        PauseRequest,
        PauseResponse,
        ResumeRequest,
        ResumeResponse,
        LastRunInfoRequest,
        LastRunInfoResponse
    }
}
