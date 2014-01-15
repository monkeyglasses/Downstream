using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using com.bitscopic.downstream.compression.gzip;
using com.bitscopic.downstream.dao.file;
using com.bitscopic.downstream.dao.sql;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using com.bitscopic.downstream.domain.svc;
using com.bitscopic.downstream.utils;
using System.Data;
using System.Configuration;
using com.bitscopic.downstream.service;

namespace com.bitscopic.downstream.net.utils
{
    public class RequestHandler
    {
        static String _errorMsg = "Invalid Request!";

        //ISqlDao _sqlDao;
        bool _locked = false; // Is this request handler allowed to dispatch jobs?

        public bool Locked
        {
            get { return _locked; }
            set { _locked = value; }
        }

        public String BatchDirectory { get; set; }

        //static readonly object _locker = new object();
        public ThreadSafeWorkStack WorkStack { get; set; }
        public ThreadSafeWorkStack ActiveExtractions { get; set; }
        public ThreadSafeWorkStack CompletedExtractions { get; set; }
        public ThreadSafeWorkStack ErroredExtractions { get; set; }
        public ThreadSafeExtractorList Extractors { get; set; }
        
        /// <summary>
        /// NOTE: RequestHandler is a singleton - there is only one ServiceState per process
        /// </summary>
        public ServiceState ServiceState;
        
        internal bool _shutdownSignal = false; // needs to be internal so we can reset it after a unit test

        #region Singleton
        public static RequestHandler getInstance()
        {
            if (_singleton == null)
            {
                lock (_locker)
                {
                    if (_singleton == null)
                    {
                        _singleton = new RequestHandler();
                    }
                }
            }
            return _singleton;
        }

        private static readonly object _locker = new object();
        private static RequestHandler _singleton;

        private RequestHandler()
        {
            // Determine what type of SQL database we are using
            //String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
            //String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
            //_sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));
        }
        #endregion

        /// <summary>
        /// Handle a request specifying from which server the request originated
        /// </summary>
        /// <param name="request"></param>
        /// <param name="server"></param>
        /// <returns></returns>
        public MessageTO handleRequest(MessageTO request, Server server)
        {
            switch (request.MessageType)
            {
                case MessageTypeTO.PauseRequest:
                    return getPauseResponse(request, server);
                case MessageTypeTO.ResumeRequest:
                    return getResumeResponse(request, server);
                case MessageTypeTO.JobStatusRequest:
                    return getJobStatusResponse(request, server);
                default :
                    return handleRequest(request);
            }
            // if we somehow get down here
            return handleRequest(request);
        }

        public MessageTO handleRequest(MessageTO request)
        {
            if (_locked)
            {
                return new MessageTO() { MessageType = MessageTypeTO.Error, Message = "The request handler is currently locked" };
            }
            MessageTO response = new MessageTO();
            //LOG.Debug("The request handler received a new request of type " + Enum.GetName(typeof(MessageTypeTO), request.MessageType));
            if (request != null && !String.IsNullOrEmpty(request.Message) &&
                request.Message.StartsWith("<ECHO>") && request.Message.EndsWith("</ECHO>"))
            {
                request.Message = request.Message.Replace("<ECHO>", "");
                request.Message = request.Message.Replace("</ECHO>", ""); // note: make sure to replace response </ECHO> since request was unchanged
                return request; // return original request
            }

            try
            {
                switch (request.MessageType)
                {
                    case MessageTypeTO.ExtractorsRequest :
                        return getExtractorsResponse(request);
                    case MessageTypeTO.JobCompletedRequest :
                        return getJobCompletedResponse(request);
                    case MessageTypeTO.JobErrorRequest :
                        return getJobErrorResponse(request);
                    case MessageTypeTO.JobStatusRequest :
                        return getJobStatusResponse(request);
                    case MessageTypeTO.LogErrorRequest :
                        logMessage(request);
                        return request;
                    case MessageTypeTO.NewJobRequest :
                        return getNewJobResponse(request);
                    case MessageTypeTO.ServerHasWorkRequest :
                        return getServerHasWorkResponse(request);
                    case MessageTypeTO.StopServerRequest :
                        return getStopJobResponse(request);
                    case MessageTypeTO.TableUploadRequest :
                        return getTableUploadResponse(request);
                    case MessageTypeTO.WorkStacksRequest :
                        return getWorkStacksResponse(request);
                    case MessageTypeTO.ZipFileUploadRequest :
                        return getZipFileUploadResponse(request);
                    case MessageTypeTO.TablesUploadRequest :
                        return getTablesUploadResponse(request);
                    case MessageTypeTO.JobPrioritizationRequest :
                        return prioritizeConfigs(request.Configurations.First().Value);
                    case MessageTypeTO.TimeToNextRunRequest :
                        return getTimeToNextRunResonse();
                    case MessageTypeTO.LastRunInfoRequest :
                        return getLastRunCompletedTimeAndBatchId();
                }
            }
            catch (Exception exc)
            {
                //LOG.Error("An unexpected error occured while handling a server request: " + exc.ToString());
                response.MessageType = MessageTypeTO.Error;
                response.Error = exc.ToString();
            }
            return response;
        }

        internal MessageTO getPauseResponse(MessageTO request, Server server)
        {
            if (server.SocketContainer.Locked)
            {
                return new MessageTO() { MessageType = MessageTypeTO.PauseResponse, Message = "Server already locked" };
            }
            else
            {
                server.SocketContainer.Locked = true;
                return new MessageTO() { MessageType = MessageTypeTO.PauseResponse, Message = "Server Locked" };
            }
        }

        internal MessageTO getResumeResponse(MessageTO request, Server server)
        {
            server.SocketContainer.Locked = false;
            return new MessageTO() { MessageType = MessageTypeTO.ResumeResponse, Message = "Server Unlocked" };
        }

        internal MessageTO getJobStatusResponse(MessageTO request, Server server)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.JobStatusResponse;

            if (server.SocketContainer.ServiceState != null)
            {
                // a bit kludgy but better than before... if the percentage complete is set (typically by the extractorservice) then return that.
                // otherwise return the ToString value of the current servicestate
                if (!String.IsNullOrEmpty(server.SocketContainer.ServiceState.PercentageComplete))
                {
                    response.Message = server.SocketContainer.ServiceState.PercentageComplete;
                }
                else
                {
                    response.Message = server.SocketContainer.ServiceState.ToString();
                }
            }

            return response;

        }

        internal MessageTO getJobStatusResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.JobStatusResponse;

            if (this.ServiceState != null)
            {
                // a bit kludgy but better than before... if the percentage complete is set (typically by the extractorservice) then return that.
                // otherwise return the ToString value of the current servicestate
                if (!String.IsNullOrEmpty(this.ServiceState.PercentageComplete))
                {
                    response.Message = this.ServiceState.PercentageComplete;
                }
                else
                {
                    response.Message = this.ServiceState.ToString();
                }
            }
            
            return response;
        }

        public MessageTO getTablesUploadResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.TablesUploadResponse;

            if (request == null || request.CompressedDataTables == null || request.CompressedDataTables.Count == 0 ||
                request.Configuration == null)
            {
                response.MessageType = MessageTypeTO.Error;
                response.Message = _errorMsg;
                return response;
            }

            try
            {
                // some times this takes a while - we don't want to wait so we'll do it in a seperate thread
                Thread extractThread = new Thread(new ParameterizedThreadStart(extractAndVerify));
                extractThread.IsBackground = true;
                // passing this ParamterizedThreadStart an ugly object but seems to be most efficient way to handle
                extractThread.Start(new KeyValuePair<ExtractorConfiguration, Dictionary<string, byte[]>>
                    (request.Configuration, request.CompressedDataTables));
            }
            catch (Exception exc)
            {
                //LOG.Error(exc);
            }
            return response;
        }

        /// <summary>
        /// Decompress the list of files, verify the CRC and save to disk
        /// </summary>
        public void extractAndVerify(object arg)
        {
            KeyValuePair<ExtractorConfiguration , Dictionary<string, byte[]>> compressedFiles = 
                (KeyValuePair<ExtractorConfiguration , Dictionary<string, byte[]>>)arg;

            GZip gzip = new GZip();
            MD5Hasher md5 = new MD5Hasher();
            FileDao fileDao = new FileDao();
            ExtractorConfiguration config = compressedFiles.Key;

            foreach (string s in compressedFiles.Value.Keys)
            {
                try
                {
                    DataTable decompressedObject = (DataTable)gzip.decompress(compressedFiles.Value[s]);
                    string hash = md5.calculateMD5(decompressedObject);
                    if (!String.Equals(s, hash, StringComparison.CurrentCultureIgnoreCase))
                    {
                        // TODO - IMPORTANT!!! NEED TO DO SOMETHING HERE - DON'T PROCEED!!!
                    }

                    fileDao.saveToFile(decompressedObject, config.ExtractMode);
                }
                catch (Exception)
                {
                    // TODO - very important we report this some how!!!
                }
            }
        }

        /// <summary>
        /// Check if the orchestrator has jobs on it's work stack. Returns the number of jobs in the message property.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public MessageTO getServerHasWorkResponse(MessageTO request)
        {
            MessageTO response = new MessageTO()
            {
                MessageType = MessageTypeTO.ServerHasWorkResponse,
                Message = "0"
            };

            //LOG.Debug("Telling client we have " + WorkStack.Count() + " jobs on the stack");
            if (!_locked && WorkStack != null && WorkStack.Count() > 0)
            {
                response.Message = WorkStack.Count().ToString();
            }
            return response;
        }

        /// <summary>
        /// This method sets the internal shutdown flag of the RequestHandler. Subsequent calls to RequestHandler.getInstance().getShutdownSignal()
        /// should return true. Note no asynchronous events occur on the parent thread or process
        /// </summary>
        /// <param name="request">MessageTO</param>
        /// <returns>MessageTO</returns>
        public MessageTO getStopJobResponse(MessageTO request)
        {
            // some notes... this is not thread safe because RequestHandler is a singleton. instead, it appears the best
            // way to enable this sort of coordination is to share local service state for a thread with it's Server
            // and just put a hook in to check for changing job status
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.StopServerResponse;
            _shutdownSignal = true;
            response.Message = "OK";
            if (this.ServiceState != null)
            {
                this.ServiceState.Status = ServiceStatus.STOPPED;
                this.ServiceState.StatusSetBy = request.HostName;
            }
            //LOG.Info("Stopping extraction job at request of orchestrator. Reason: " + request.Message);
            return response;
        }

        /// <summary>
        /// Get all the extractors currently running.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public MessageTO getExtractorsResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.ExtractorsResponse;
            response.Extractors = Extractors.GetExtractors();
            return response;
        }

        /// <summary>
        /// Return all the work stacks
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public MessageTO getWorkStacksResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.WorkStacksResponse;

            //LOG.Debug(WorkStack.Count() + " jobs on the workstack");
            //LOG.Debug(ActiveExtractions.Count() + " active jobs");
            //LOG.Debug(ErroredExtractions.Count() + " errored jobs");
            //LOG.Debug(CompletedExtractions.Count() + " completed jobs");

            response.Configurations = new Dictionary<string, IList<ExtractorConfiguration>>();
            response.Configurations.Add("Queued", new List<ExtractorConfiguration>());
            response.Configurations.Add("Active", new List<ExtractorConfiguration>());
            response.Configurations.Add("Errored", new List<ExtractorConfiguration>());
            response.Configurations.Add("Completed", new List<ExtractorConfiguration>());

            if (WorkStack != null && WorkStack.Count() > 0)
            {
                WorkStack.CopyTo(response.Configurations["Queued"]);
            }

            if (ActiveExtractions != null && ActiveExtractions.Count() > 0)
            {
                response.Configurations["Active"] = ActiveExtractions.CopyTo(response.Configurations["Active"]);
            }

            if (ErroredExtractions != null && ErroredExtractions.Count() > 0)
            {
                response.Configurations["Errored"] = ErroredExtractions.CopyTo(response.Configurations["Errored"]);
            }

            if (CompletedExtractions != null && CompletedExtractions.Count() > 0)
            {
                response.Configurations["Completed"] = CompletedExtractions.CopyTo(response.Configurations["Completed"]);
            }

            response.Message = gov.va.medora.utils.JsonUtils.Serialize<TaggedExtractorConfigArrays>
                (new TaggedExtractorConfigArrays(response.Configurations));

            response.Configurations = null; // null this since we serialized the results to a JSON string 

            return response;
        }

        public MessageTO getZipFileUploadResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.ZipFileUploadResponse;

            if (request == null || request.ZippedFile == null || request.ZippedFile.Length == 0)
            {
                response.MessageType = MessageTypeTO.Error;
                response.Message = _errorMsg;
                return response;
            }

            try
            {
                // some times this takes a while - we don't want to wait so we'll do it in a seperate thread
                FileDao fileDao = new FileDao();
                Thread extractThread = new Thread(new ParameterizedThreadStart(fileDao.extractFiles));
                extractThread.IsBackground = true;
                extractThread.Start(new KeyValuePair<string, byte[]>(request.Message, request.ZippedFile));
            }
            catch (Exception exc)
            {
                //LOG.Error(exc);
            }
            return response;
        }

        public MessageTO getTableUploadResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.TableUploadResponse;

            if (request == null || request.VistaData == null || String.IsNullOrEmpty(request.VistaData.TableName) ||
                request.VistaData.Rows == null || request.VistaData.Rows.Count <= 0 || request.Configuration == null)
            {
                //LOG.Debug("Received an invalid table upload request - notifying client");
                response.MessageType = MessageTypeTO.Error;
                response.Error = "Invalid table upload request!";
                return response;
            }

            try
            {

                FileDao fileDao = new FileDao();
                response.Message = fileDao.saveToFile(request.VistaData, request.Configuration.ExtractMode);
            }
            catch (Exception exc)
            {
                response.MessageType = MessageTypeTO.Error;
                response.Error = exc.ToString();
            }
            return response;
        }

        /// <summary>
        /// Check to see if any post processing needs to happen for this file. If so, do it.
        /// </summary>
        /// <param name="request">The last request we serviced</param>
        private void postProcess(object obj)
        {
            MessageTO request = (MessageTO)obj;

            // Set up
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.JobCompletedResponse;

            // Protect
            if (request == null || String.IsNullOrEmpty(request.Configuration.SiteCode) ||
                String.IsNullOrEmpty(request.Configuration.QueryConfigurations.RootNode.Value.File))
            {
                return;
            }

            // Do our work
            try
            {
                String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
                String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
                ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));

                //if (request.Configuration.ON_COMPLETE != null)
                //{
                    if (ActiveExtractions.ExistsByFile(request.Configuration.QueryConfigurations.RootNode.Value.File)
                        ||
                        WorkStack.ExistsByFile(request.Configuration.QueryConfigurations.RootNode.Value.File)
                        ) { } else
                    {
                        //sqlDao.executeDelimited(request.Configuration.ON_COMPLETE, 5 * 60 * 1000);
                        //// for each query configuration, disable indexes on the file
                        //IList<string> distinctFiles = new List<string>();
                        //parseDistinctFiles(request.Configuration.QueryConfigurations.RootNode, distinctFiles);
                        //foreach (string file in distinctFiles)
                        //{
                        //    sqlDao.enableIndexes(file);
                        //}
                        // TODO: Add support for the SQL server version of this
                        //sqlDao.executeStoredProcedureNoArguments(request.Configuration.QueryConfigurations.RootNode.Value.File + "_END", 10 * 60 * 1000);
                        sqlDao.postProcess(request.Configuration.QueryConfigurations.RootNode);
                    }
                //}
            }
            catch
            {
                // TODO: REQ HANDLER NEEDS A LOGGING MECHANISM THAT IS THREAD-SAFE
            }
        }

        // TODO: THIS SHOULD BE REFACTORED INTO A UTILITY CLASS SINCE IT IS USED BY ORCSERVICE
        /// <summary>
        /// Parse out the distinct files associated with this tree of configurations
        /// </summary>
        /// <param name="node">The node we are examining</param>
        /// <param name="dFiles">The list to hold the files names</param>
        private void parseDistinctFiles(TreeNode<QueryConfiguration> node, IList<string> dFiles)
        {
            dFiles.Add(node.Value.File);
            if (node.Value.Gets_Alignment != null && !node.Value.Gets_Alignment.Equals(String.Empty) && node.Value.Gets_Alignment.Equals("VERTICAL"))
            {
                dFiles.Add(node.Value.File + "_KEYVALUE");
            }
            foreach (TreeNode<QueryConfiguration> child in node.Children)
            {
                parseDistinctFiles(child, dFiles);
            }
        }

        public MessageTO getJobCompletedResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.JobCompletedResponse;

            if (request == null || String.IsNullOrEmpty(request.Configuration.SiteCode) ||
                String.IsNullOrEmpty(request.Configuration.QueryConfigurations.RootNode.Value.File))
            {
                //LOG.Debug("Received an invalid job completed request from the client - unable to process: " + request.Configuration.ToString());
                logging.Log.LOG("Problem with job complete request!");
                response.MessageType = MessageTypeTO.Error;
                response.Error = "Invalid request!";
                return response;
            }

            try
            {
                String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
                String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
                ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));
                if (request.ExtractorReport != null)
                {
                    try
                    {
                        logging.Log.LOG("Saving successful job report...");
                        sqlDao.saveReport(request.ExtractorReport);
                    }
                    catch (Exception exc)
                    {
                        logging.Log.LOG("Unable to save extractors report: " + request.Configuration.SiteCode + " - " + request.Configuration.QueryConfigurations.RootNode.Value.File + ", " + exc.Message);
                    }
                }
                // get last IEN from message and save to tracking table
                sqlDao.saveLastIen(request.Configuration.SiteCode, request.Configuration.QueryConfigurations.RootNode.Value.File, request.ExtractorReport.StartIen, request.ExtractorReport.LastIen, request.ExtractorReport.BatchId);
                // unlock site from locks table
                sqlDao.unlockSite(request.Configuration.SiteCode, request.Configuration.QueryConfigurations.RootNode.Value.File);
                // the remove function keys off the site code and vista file - those are the only params we need
                Extractors.Remove(new Extractor("", 0, request.Configuration.SiteCode, request.Configuration.QueryConfigurations.RootNode.Value.File, DateTime.Now));
                CompletedExtractions.Push(request.Configuration);
                ActiveExtractions.Remove(request.Configuration);
                // all done with site?
                try
                {
                    checkSiteCompleteAndTrigger(request.ExtractorReport.BatchId, request.Configuration.SiteCode, request.Configuration.QueryConfigurations.RootNode.Value.File);
                }
                catch (Exception triggerExc) // shouldn't fail job complete request if there is a problem with this - we just won't create the trigger
                {
                    logging.Log.LOG("An unexpected error occured when checking if it was ok to create the trigger for site " + request.Configuration.SiteCode + "\r\n\r\n" + triggerExc.ToString());
                }
                response.Message = "Nice Work!";
            }
            catch (Exception exc)
            {
                logging.Log.LOG(exc.Message);
                response.MessageType = MessageTypeTO.Error;
                response.Error = exc.ToString();
            }

            // handle post process in separate thread
            Thread postProcessThread = new Thread(new ParameterizedThreadStart(RequestHandler.getInstance().postProcess));
            postProcessThread.Start(request);

            return response;
        }

        internal void checkSiteCompleteAndTrigger(String extractorBatchId, String sitecode, String vistaFile)
        {
            // if no queued jobs and no active jobs for site
            if (!WorkStack.ContainsBySite(sitecode) && !ActiveExtractions.ContainsBySite(sitecode))
            {
                // TODO!!!! NEED TO CHECK AND MAKE SURE ALL JOBS FINISHED SUCCESSFULLY FOR THIS SIT AND BATCH AND ROLL BACK LAST IENS WRITTEN TO MDO IEN TRACKING AND ALSO NOT CREATE TRIGGER FILE
                String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
                String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
                ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));

                // TODO - need to make error handling a bit more sophisticated so it can deal with non-critical file failures in certain modes and similar non-failure type events
                if (sqlDao.hasBatchAndSiteErrors(extractorBatchId, sitecode))
                {
                    sqlDao.saveExceptional(new domain.reporting.Exceptional() { Code = domain.reporting.ErrorCode.TRIGGER_NOT_CREATED, Message = "Trigger not created due to error for site/batch" }, extractorBatchId, sitecode, vistaFile);
                    logging.Log.LOG("Found error for site and didn't create batch file - logged event to database");
                    return;
                }

                //IList<Int32> errorCodes = sqlDao.getErrorsForBatchAndSite(extractorBatchId, sitecode);
                //foreach (Int32 code in errorCodes)
                //{
                //    if (code > 1) // TODO - NOT CRAZY ABOUT THIS!!! Need better way to encapsulate what error codes are acceptable and which are not. Will at least serve as proof of concept for now
                //    {
                //        // don't create trigger file! need to log event somehow
                //        domain.reporting.OrchestratorReport noTriggerRpt = new domain.reporting.OrchestratorReport(extractorBatchId);
                //        noTriggerRpt.addError(String.Format("Not creating trigger for site {0}, batch {1} due to error code > 1", sitecode, extractorBatchId));
                //        sqlDao.saveReport(noTriggerRpt);
                //        return;
                //    }
                //}


                // woot! site is finished - we can go ahead and trigger
                logging.Log.LOG("Site complete!!! Creating trigger for " + sitecode);
                IFileDao fileDao = new FileDaoFactory().getFileDao();
                fileDao.setExtractsDirectory(sitecode, this.BatchDirectory);
                fileDao.createMarkerFile();
            }
        }

        public MessageTO getJobErrorResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.JobErrorResponse;
            if (request == null || request.Configuration == null || !request.Configuration.isCompleteConfiguration())
            {
                response.MessageType = MessageTypeTO.Error;
                response.Error = "Incomplete ExtractorConfiguration parameter on request";
                logging.Log.LOG("The ExtractorConfiguration object sent to the job error request handler is incomplete! Unable to process request");
                //LOG.Error("The ExtractorConfiguration object sent to the job error request handler is incomplete! Unable to process request: " + request.Configuration.ToString());
                return response;
            }
            if (WorkStack == null)
            {
                logging.Log.LOG("The work stack is null for the job error request. Was everything setup correctly?");
                //LOG.Debug("The work stack is null - was everything set up correctly?");
                response.MessageType = MessageTypeTO.Error;
                response.Error = "WorkStack has not been initialized";
                return response;
            }

            // the remove function keys off the site code and vista file - those are the only params we need
            Extractors.Remove(new Extractor("", 0, request.Configuration.SiteCode, request.Configuration.QueryConfigurations.RootNode.Value.File, DateTime.Now));
            ActiveExtractions.Remove(request.Configuration);
            ErroredExtractions.Push(request.Configuration);

            // bug fix: some edge cases will allow duplicate configs to be added to stack - need to check not already there
            //bool alreadyOnStack = false;
            //if (WorkStack.Contains(request.Configuration))
            //{
            //    //LOG.Debug("It looks like this job was already on the stack - no need to add it again");
            //    alreadyOnStack = true;
            //}
            // decided to temporarily stop adding errored jobs back to the stack to check for patterns in error site codes
            //if (!alreadyOnStack)
            //{
            //    LOG.Debug("Adding a job back to the work stack since the client reported an error while processing");
            //    ErroredExtractions.Push(request.Configuration);
            //    ActiveExtractions.Remove(request.Configuration);
            //    WorkStack.Push(request.Configuration);
            //}
            // end bug fix

            try
            {
                String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
                String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
                ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));
                if (request.ExtractorReport != null)
                {
                    try
                    {
                        logging.Log.LOG("Saving error report...");
                        sqlDao.saveReport(request.ExtractorReport);
                    }
                    catch (Exception exc)
                    {
                        //LOG.Error("Unable to save an extractor's report!", exc);
                    }
                }
                sqlDao.unlockSite(request.Configuration.SiteCode, request.Configuration.QueryConfigurations.RootNode.Value.File);
                //LOG.Debug("Successfully unlocked job so another client can process");

                // should check site completion on job error reports too
                try
                {
                    checkSiteCompleteAndTrigger(request.ExtractorReport.BatchId, request.Configuration.SiteCode, request.Configuration.QueryConfigurations.RootNode.Value.File);
                }
                catch (Exception te) 
                { /* problems with trigger file shouldn't effect extractor */
                    logging.Log.LOG("An error occured when checking trigger file creation criteria: " + te.ToString());
                }
            }
            catch (Exception exc)
            {
                logging.Log.LOG("The call to unlock the extraction job for site " + request.Configuration.SiteCode +
                    ", file " + request.Configuration.QueryConfigurations.RootNode.Value.File + " has failed unexpectedly");
                logging.Log.LOG(exc.Message);

                response.MessageType = MessageTypeTO.Error;
                response.Error = exc.ToString();
                return response;
            }
            return response;
        }

        /// <summary>
        ///  For new job requests, do the following:
        ///      1. Make sure we can call client back on specified hostname
        ///      2. Pop a job off the work stack
        ///      3. Try locking the site/file from the popped job (put job back on stack if fail)
        ///      4. Send response back to client
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public MessageTO getNewJobResponse(MessageTO request)
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.NewJobResponse;

            if (String.Equals(request.HostName, "SansCallback") && request.ListeningPort == 0)
            {
                // this is from a test - don't require a callback
            }
            else
            {
                try
                {
                    //LOG.Debug("Received a new job response - first checking to make sure we can communicate with the client");
                    Client c = new Client();
                    c.connect(request.HostName, request.ListeningPort);
                    c.disconnect();
                    //LOG.Debug("Successfully connected to the client! Going to try sending the client a job now...");
                }
                catch (Exception exc)
                {
                    response.MessageType = MessageTypeTO.Error;
                    response.Error = exc.ToString();
                    //LOG.Debug("Couldn't call back client who requested new job!", exc);
                    return response;
                }
            }

            if (this.WorkStack == null)
            {
                //LOG.Debug("The work stack is null! Has the application been initialized properly?");
                response.MessageType = MessageTypeTO.Error;
                response.Error = "The WorkStack has not been initialized!";
                return response;
            }
            // I think we were opening up a race condition here - now we just try and pop a job and see if we got one
            // instead of making a call for the count and then another to pop a job
            //if (WorkStack.Count() == 0)
            //{
            //    LOG.Debug("Looks like there are no more jobs on the work stack! Tell the client thanks but nothing to do");
            //    response.Message = "No more jobs on the stack!";
            //    return response;
            //}

            //LOG.Debug("Found a job for the client - popping it off the stack");

            // Don't process new jobs if we are currently locked
            if (_locked)
            {
                response.Message = "No more jobs on the stack!";
                return response;
            }

            //ExtractorConfiguration responseConfig = WorkStack.PopSiteUnique(ActiveExtractions);
            ExtractorConfiguration responseConfig = WorkStack.Pop();
            if (responseConfig == null)
            {
                //LOG.Debug("Looks like there are no more jobs on the work stack! Tell the client thanks but nothing to do");
                response.Message = "No more jobs on the stack!";
                return response;
            }
            String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
            String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
            ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));
            try
            {
                if (!sqlDao.lockSite(responseConfig.SiteCode, responseConfig.QueryConfigurations.RootNode.Value.File,
                    request.HostName, Convert.ToInt16(request.ListeningPort)))
                {
                    //LOG.Debug("Unable to lock the new job for the client! Tell them we couldn't assign a new job");
                    response.MessageType = MessageTypeTO.Error;
                    response.Error = "Unable to lock site!";
                    return response;
                }
            }
            catch (Exception exc)
            {
                WorkStack.Push(responseConfig); // put it back on stack if error occured

                response.MessageType = MessageTypeTO.Error;
                response.Error = exc.ToString();
                //LOG.Error(exc);
                return response;
            }

            ActiveExtractions.Push(responseConfig);
            response.Configuration = responseConfig;
            response.Extractor = new Extractor(request.HostName, request.ListeningPort,
                response.Configuration.SiteCode, response.Configuration.QueryConfigurations.RootNode.Value.File, DateTime.Now);

            Extractors.Add(response.Extractor);
            //LOG.Debug("Successfully obtained a new job for our client and locked it in the database! Sending...");
            response.Extractor.Configuration = responseConfig;

            // now passing the extractor a directory name in which to dump all extractions
            response.Message = this.BatchDirectory;
            // end passing dir name

            /// TBD - maybe provide a start value in the event a vista extraction stopped in the middle of a file and 
            /// the extractor 
            //try
            //{
            //    FileDao fileDao = new FileDao();
            //    string lastFileIen = fileDao.getLastIen(response.Configuration.SiteCode, response.Configuration.VistaFile);
            //    int lastIen = Convert.ToInt32(lastFileIen);
            //    if (lastIen > 0)
            //    {
            //        response.Extractor.
            //    }
            //}
            //    catch (Exception exc)
            //    {

            //    }

            return response;
        }

        public void logMessage(MessageTO request)
        {
            //LOG.Error("An extraction job uploaded the following error: " + request.Error);
            try
            {
                System.Net.Mail.SmtpClient mail = new System.Net.Mail.SmtpClient("smtp.va.gov", 25);
                mail.SendAsync("DownstreamOrchestrator@va.gov", "kevin.seiter@va.gov", "Log Error Request From Client", request.Error, new object());
            }
            catch (Exception exc)
            {
                //LOG.Error("Unable to send email about error log request!", exc);
            }
        }

        public MessageTO prioritizeConfigs(IList<ExtractorConfiguration> configs)
        {
            if (this.WorkStack != null && this.WorkStack.Count() > 0)
            {
                this.WorkStack.Prioritize(configs);
            }

            return new MessageTO() { MessageType = MessageTypeTO.JobPrioritizationResponse, Message = "OK" };
        }

        public bool getShutdownSignal()
        {
            return _shutdownSignal;
        }

        public MessageTO getTimeToNextRunResonse()
        {
            MessageTO response = new MessageTO();
            response.MessageType = MessageTypeTO.TimeToNextRunResponse;

            response.Message = this.ServiceState.NextRun.ToString();

            return response;
        }

        public MessageTO getLastRunCompletedTimeAndBatchId()
        {
            String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
            String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
            ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));
            return new MessageTO() 
            { 
                MessageType = MessageTypeTO.LastRunInfoResponse,
                Message = gov.va.medora.utils.JsonUtils.Serialize<KeyValuePair<String, DateTime>>(sqlDao.getLastRunCompletedTimeAndBatchId())
            };
        }
    }
}
