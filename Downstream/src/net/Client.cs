using System;
using System.Net;
using System.Linq;
using System.Net.Sockets;
using System.Collections.Specialized;
using System.Text;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.reporting;
using com.bitscopic.downstream.net.utils;
using System.Configuration;
using System.Xml;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections.Generic;
using System.IO;
using System.Data;

namespace com.bitscopic.downstream.net
{
    public class Client
    {
        IPAddress _serverIp;
        Int32 _serverPort;
        //IPAddress _localIp;
        //Int32 _listeningPort;
        TcpClient _connection;

        internal String getHostname() { return _serverIp.ToString(); }
        internal Int32 getPort() { return _serverPort; }

        public bool IsConnected
        {
            get
            {
                if (_connection == null)
                {
                    return false;
                }
                return _connection.Connected;
            }
        }

        public Client()
        {
            _serverIp = IPv4Helper.getIPv4AddressForHost(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorHostName]);
            _serverPort = Convert.ToInt32(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorListeningPort]);
        }

        public Client(string hostname, Int32 port)
        {
            _serverIp = IPv4Helper.getIPv4AddressForHost(hostname);
            _serverPort = port;
        }

        /// <summary>
        /// Connect to the orchestrator specified in the app.config file
        /// </summary>
        public void connect()
        {
            connect(_serverIp.ToString(), _serverPort);
        }

        /// <summary>
        /// Connect to a specified endpoint
        /// </summary>
        /// <param name="hostname">The hostname to connect to</param>
        /// <param name="port">The port the host is listening on</param>
        public void connect(string hostname, int port)
        {
            // if args not supplied then set to config file values
            if (String.IsNullOrEmpty(hostname))
            {
                hostname = _serverIp.ToString();
            }
            if (port == 0)
            {
                port = _serverPort;
            }
            IPAddress remoteHost = IPv4Helper.getIPv4Address();

            TcpClient client = new TcpClient();
            client.Connect(hostname, port);
            _connection = client;
        }

        public string sendMessage(string message)
        {
            return sendMessage(message, true, false);
        }

        public string sendMessage(string message, bool disconnect)
        {
            return sendMessage(message, disconnect, false);
        }

        /// <summary>
        /// Submit a query to the connected socket
        /// </summary>
        /// <param name="request">The request string to submit</param>
        /// <param name="disconnect">Will disconnect socket after query has completed if set to true</param>
        /// <param name="echoRequest">Submits specially formatted request that Server class will simply send back untouched</param>
        /// <returns>The response from the query</returns>
        public string sendMessage(string message, bool disconnect, bool echoRequest)
        {
            MessageTO messageTO = query(new MessageTO() { Message = message }, disconnect, echoRequest);
            return messageTO.Message;
        }

        /// <summary>
        /// Serialize the MessageTO and send it to the connected socket
        /// </summary>
        /// <param name="messageTO"></param>
        /// <param name="disconnect"></param>
        /// <param name="echoRequest"></param>
        /// <returns>The MessageTO received as a response to the MessageTO sent</returns>
        internal MessageTO query(MessageTO messageTO, bool disconnect, bool echoRequest)
        {
            try
            {
                if (_connection == null || !_connection.Connected)
                {
                    connect(_serverIp.ToString(), _serverPort);
                }
                if (messageTO == null)
                {
                    throw new ArgumentNullException("Must supply a message to send");
                }
                if (echoRequest)
                {
                    messageTO.Message = "<ECHO>" + messageTO.Message + "</ECHO>";
                }

                NetworkStream writer = _connection.GetStream();
                BinaryFormatter bf = new BinaryFormatter();
                MemoryStream bytesToSend = new MemoryStream();
                bf.Serialize(bytesToSend, messageTO);

                //byte EOT = System.Text.Encoding.ASCII.GetBytes("\x04")[0];

                //bytesToSend.WriteByte(EOT); // add EOT

                String lengthHeader = String.Concat(bytesToSend.Length.ToString(), "|");

                writer.Write(System.Text.ASCIIEncoding.ASCII.GetBytes(lengthHeader), 0, lengthHeader.Length); // write length and a pipe as header first
                writer.Write(bytesToSend.GetBuffer(), 0, (Int32)bytesToSend.Length); // then write the rest
                writer.Flush();

                // sent message - now receive response
                int responseBufferSize = 8192; // create a buffer the size of the expected read - never expect big messages from server so this should be ok
                byte[] chunk = new byte[responseBufferSize];

                NetworkStream reader = _connection.GetStream();
#if RELEASE
                reader.ReadTimeout = 300000; // give client 5 minutes to send the data and receive a response
#endif
                MemoryStream responseBytes = new MemoryStream();

                int bytesRead = reader.Read(chunk, 0, responseBufferSize);

                // first get length header
                int msgLength = 0;
                for (int i = 0; i < bytesRead; i++)
                {
                    if (chunk[i] == '\x7c')
                    {
                        msgLength = Convert.ToInt32(System.Text.ASCIIEncoding.ASCII.GetString(chunk.Take(i).ToArray()));
                        bytesRead = bytesRead - i - 1; // -1 for the pipe too
                        break;
                    }
                }

                chunk = chunk.Skip(msgLength.ToString().Length + 1).ToArray(); // now reset chunk to skip the header

                responseBytes.Write(chunk, 0, bytesRead);

                while (responseBytes.Length < msgLength)
                {
                    chunk = new byte[responseBufferSize];
                    bytesRead = reader.Read(chunk, 0, responseBufferSize);
                    responseBytes.Write(chunk, 0, bytesRead);
                }

                if (disconnect)
                {
                    this.disconnect();
                }

                bf = new BinaryFormatter();
                responseBytes.Position = 0; // reset memory stream position for deserialization
                MessageTO responseTO = (MessageTO)bf.Deserialize(responseBytes);

                return responseTO;
            }
            catch (Exception exc)
            {
                return new MessageTO
                {
                    MessageType = MessageTypeTO.Error,
                    Error = exc.ToString()
                };
            }
        }

        /// <summary>
        /// Disconnect the client
        /// </summary>
        public void disconnect()
        {
            try
            {
                if (_connection != null && _connection.Connected)
                {
                    NetworkStream stream = _connection.GetStream();
                    if (stream != null)
                    {
                        //stream.Flush();
                        stream.Close();
                    }
                    _connection.Close();
                }
            }
            catch (Exception) { }
        }

        /// <summary>
        /// Send a request to the server to ask if any more jobs are available on it's work stack
        /// </summary>
        /// <returns>Returns true if server has work items, false otherwise</returns>
        public bool sendServerHasWorkRequest()
        {
            MessageTO message = new MessageTO();
            message.MessageType = MessageTypeTO.ServerHasWorkRequest;
            MessageTO response = submitRequest(message);
            if (response != null)
            {
                //LOG.Debug("Received " + Enum.GetName(typeof(MessageTypeTO), response.MessageType) + " message type");
                if (response.MessageType == MessageTypeTO.Error)
                {
                    //LOG.Debug(response.Error);
                }
                //LOG.Debug("Server reported " + response.Message + " jobs");
            }
            int trash = 0;
            // response should be server has work type, message should contain the number of items on the 
            // work stack, the message should be a number and the number should be > 0
            if (response != null &&
                response.MessageType == MessageTypeTO.ServerHasWorkResponse &&
                !String.IsNullOrEmpty(response.Message) &&
                Int32.TryParse(response.Message, out trash) &&
                trash > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Send a request to either an extractor's or the orchestrator's server to stop the process
        /// </summary>
        /// <param name="reason">The reason for the request</param>
        /// <returns>True if the request was acknoledged, false otherwise</returns>
        public bool sendStopServerRequest(string reason)
        {
            if (String.IsNullOrEmpty(reason))
            {
                return false;
            }
            MessageTO message = new MessageTO()
            {
                MessageType = MessageTypeTO.StopServerRequest,
                Message = reason
            };
            MessageTO response = submitRequest(message);
            if (response == null || response.MessageType != MessageTypeTO.StopServerResponse)
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Send a request for acknoledgement of job error
        /// </summary>
        /// <param name="config">The extraction routine's configuration</param>
        /// <param name="error">Any error message to be passed</param>
        /// <returns>True if acknoledged, false otherwise</returns>
        public bool sendJobErrorRequest(ExtractorConfiguration config, ExtractorReport report)
        {
            //LOG.Debug("Sending the orchestrator a job error request");
            MessageTO message = new MessageTO()
            {
                Message = "Job Error!",
                //Extractor = new Extractor(extractor.HostName, extractor.ListeningPort, 
                //    extractor.SiteCode, extractor.VistaFile, extractor.Timestamp),
                Configuration = config,
                ExtractorReport = report,
                //Error = report.ToString(),
                //HostName = extractor.HostName,
                //ListeningPort = extractor.ListeningPort,
                MessageType = MessageTypeTO.JobErrorRequest
            };
            MessageTO response = submitRequest(message);
            if (response == null || response.MessageType != MessageTypeTO.JobErrorResponse)
            {
                ////LOG.Debug("The orchestrator did not successfully acknowledge our job error request!");
                return false;
            }
            else
            {
                //LOG.Debug("The orchestrator successfully acknowledged our job error response! Better luck next time...");
                return true;
            }
        }

        /// <summary>
        /// Send a completed message
        /// </summary>
        /// <param name="config"></param>
        public bool sendJobCompletedRequest(ExtractorConfiguration config, ExtractorReport report, String lastIen)
        {
           // LOG.Debug("Sending orchestrator a notice the job has beeen completed...");
            MessageTO message = new MessageTO()
            {
                ExtractorReport = report,
                Message = lastIen,
                Configuration = config,
                MessageType = MessageTypeTO.JobCompletedRequest
            };
            MessageTO response = submitRequest(message);
            if (response == null || response.MessageType != MessageTypeTO.JobCompletedResponse)
            {
                //LOG.Debug("The orchestrator did not successfully acknowledge our job completed notice!");
                return false;
            }
            else
            {
                //LOG.Debug("The orchestrator got our job completed notice! We are such a good worker!");
                return true;
            }
        }

        /// <summary>
        /// Ask the orchestrator for a new extraction job
        /// </summary>
        /// <returns></returns>
        public MessageTO sendNewJobRequest(string localHostName, Int32 listeningPort)
        {
            //LOG.Debug("Sending a new job request to the orchestrator");
            MessageTO message = new MessageTO()
            {
                HostName = localHostName,
                ListeningPort = listeningPort,
                Message = "New Job",
                MessageType = MessageTypeTO.NewJobRequest
            };
            message.Extractor = new Extractor(message.HostName, message.ListeningPort, "", "", new DateTime());
            MessageTO response = submitRequest(message);
            //LOG.Debug("Successfully received a new job response from the orchestrator!");
            return response;
        }

        public MessageTO sendLogErrorRequest(string errorMessage)
        {
            //LOG.Debug("Sending a log error request to the orchestrator");
            MessageTO message = new MessageTO()
            {
                Error = errorMessage,
                MessageType = MessageTypeTO.LogErrorRequest
            };
            return submitRequest(message);
        }

        /// <summary>
        /// Send a request to upload a DataTable of Vista data to the orchestrator service
        /// </summary>
        /// <param name="vistaData">The Vista data in a DataTable object</param>
        /// <param name="config">The extractor's configuration</param>
        /// <returns>Returns the file name created on the orchestrator server if successful. Returns null if there was an error</returns>
        public string sendVistaDataUploadRequest(DataTable vistaData, ExtractorConfiguration config)
        {
            //LOG.Debug("Sending a request to upload our extraction results to the orchestrator");
            MessageTO message = new MessageTO()
            {
                VistaData = vistaData,
                MessageType = MessageTypeTO.TableUploadRequest,
                Configuration = config
            };
            MessageTO response = submitRequest(message);
            if (response.MessageType == MessageTypeTO.TableUploadResponse)
            {
                //LOG.Debug("Successfully uploaded our extraction results!");
                return response.Message;
            }
            else
            {
                //LOG.Debug("Uh-oh! Our upload seems to have failed...");
                return null;
            }
        }

        public bool send7zipFileUploadRequest(string checksum, byte[] fileBytes)
        {
           // LOG.Debug("Sending a request to upload our zipped extraction results to the orchestrator");
            MessageTO message = new MessageTO
            {
                Message = checksum,
                ZippedFile = fileBytes,
                MessageType = MessageTypeTO.ZipFileUploadRequest
            };
            MessageTO response = submitRequest(message);
            if (response.MessageType == MessageTypeTO.ZipFileUploadResponse)
            {
                //LOG.Debug("Successfully uploaded our zip file!");
                return true;
            }
            else
            {
                //LOG.Debug("Uh-oh! Some unknown error occurred while uploading our zip file!");
                return false;
            }
        }

        public bool sendCompressedFilesUploadRequest(Dictionary<string, byte[]> compressedFiles, ExtractorConfiguration config)
        {
            MessageTO message = new MessageTO
            {
                CompressedDataTables = compressedFiles,
                MessageType = MessageTypeTO.TablesUploadRequest,
                Configuration = config
            };
            MessageTO response = submitRequest(message);
            if (response == null || response.MessageType != MessageTypeTO.TablesUploadResponse)
            {
                return false;
            }
            return true;
        }

        public IList<Extractor> sendGetExtractorsRequest()
        {
            MessageTO request = new MessageTO();
            request.MessageType = MessageTypeTO.ExtractorsRequest;

            MessageTO response = submitRequest(request);
            if (response == null || response.MessageType != MessageTypeTO.ExtractorsResponse)
            {
                return null;
            }
            return response.Extractors;
        }

        /// <summary>
        /// Get a generic list of configurations from the orchestrator
        /// </summary>
        /// <returns></returns>
        public String sendGetWorkListsRequest()
        {
            MessageTO request = new MessageTO()
            {
                MessageType = MessageTypeTO.WorkStacksRequest,
            };
            MessageTO response = submitRequest(request);
            //if (response == null || response.Configurations == null)
            //{
            //    return null;
            //}
            if (response == null || String.IsNullOrEmpty(response.Message))
            {
                return null;
            }
            
            return response.Message; // now contains JSON representation of TaggedExtractorConfigurationArrays
        }

        /// <summary>
        /// Send a request for the completion percentage of a client extractor
        /// </summary>
        /// <returns>A string representing the percentage complete of a client extractor</returns>
        public string sendJobStatusRequest()
        {
            MessageTO request = new MessageTO();
            request.MessageType = MessageTypeTO.JobStatusRequest;
            MessageTO response = submitRequest(request);
            if (response != null)
            {
                return response.Message;
            }
            return "0";
        }

        public MessageTO submitRequest(MessageTO message)
        {
            try
            {
                return query(message, true, false);
            }
            catch (Exception exc)
            {
                //LOG.Error("Unable to send message: " + message.ToString(), exc);
                return null;
            }
        }

        public MessageTO sendPrioritizationRequest(IList<ExtractorConfiguration> configs)
        {
            MessageTO request = new MessageTO();
            request.MessageType = MessageTypeTO.JobPrioritizationRequest;
            request.Configurations = new Dictionary<string, IList<ExtractorConfiguration>>();
            request.Configurations.Add("PRIORITIZE", configs);

            return submitRequest(request);
        }
    }
}
