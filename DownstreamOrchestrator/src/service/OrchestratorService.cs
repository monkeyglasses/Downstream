using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using com.bitscopic.downstream.dao.sql;
using com.bitscopic.downstream.dao.vista;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using com.bitscopic.downstream.domain.reporting;
using com.bitscopic.downstream.logging;
using com.bitscopic.downstream.net;
using com.bitscopic.downstream.net.utils;
using com.bitscopic.downstream.utils;
using System.Configuration;
using System.Threading;
using com.bitscopic.downstream.dao.file;
using System.ComponentModel;
using NCrontab;

namespace com.bitscopic.downstream.service
{
    public class OrchestratorService : AbstractService
    {
        //static ILog LOG = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        IPAddress _myIp;
        string _myHostName;
        Int32 _listeningPort;
        IList<string> _vhaSites;
        IList<ExtractorConfiguration> _configurations;
        Dictionary<string, Extractor> _activeJobsAtStartUp;
        // some edge cases cause some configs to be missing - we will use the _allConfigs object to pass to
        // the SQL service in another thread to make sure we have all our SQL connection strings and config data
        static ThreadSafeWorkStack _allConfigs;
        static ThreadSafeWorkStack _workStack;
        static ThreadSafeWorkStack _activeExtractions;
        static ThreadSafeWorkStack _completedExtractions;
        static ThreadSafeWorkStack _erroredExtractions;
        static ThreadSafeExtractorList _extractors;
        static Int32 _cronSchedule = 15;
        OrchestratorReport _report = new OrchestratorReport(""); // don't have batch ID yet - set it below at iteration
        ISqlDao _sqlDao;
        //Thread _sqlThread;
        Server _server;
        ServiceState _serviceState = new ServiceState();
        Int32 _dashboardCleanupTime = 0;

        public OrchestratorReport Report { get { return _report; } }

        public OrchestratorService()
        {
            Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.DashboardCleanupTime], out _dashboardCleanupTime);
            Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.CronSchedule], out _cronSchedule);
            // Set up
            _allConfigs = new ThreadSafeWorkStack();
            _extractors = new ThreadSafeExtractorList();
            _workStack = new ThreadSafeWorkStack();
            _activeExtractions = new ThreadSafeWorkStack();
            _completedExtractions = new ThreadSafeWorkStack();
            _erroredExtractions = new ThreadSafeWorkStack();
            _myHostName = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorHostName];
            _report.OrchestratorHostName = _myHostName;
            _listeningPort = Convert.ToInt32(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorListeningPort]);
            _myIp = IPv4Helper.getIPv4Address();

            // Determine what type of SQL database we are using
            String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
            String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
            _sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));
            
            // make own unique list of sitecodes
            VistaDao vistaDao = new VistaDao();
            string[] sitecode = new string[vistaDao.SiteCodes.Count];
            vistaDao.SiteCodes.CopyTo(sitecode, 0);
            _vhaSites = new List<string>(sitecode);

            if (_vhaSites == null || _vhaSites.Count == 0)
            {
                _report.addException(new ApplicationException("Failed to open the VHA site file!"));
                throw new ApplicationException("Failed to open the VHA site file!");
            }
        }

        protected override void start()
        {
        }

        protected override void stop()
        {
        }


        /// <summary>
        /// Run the orchestrator service
        /// </summary>
        /// <param name="blocking">If set to false, this function will return after a successful setup.
        /// If set to true, it will complete the setup but enter a permanent loop that periodically
        /// verifies the application is still running as expected.</param>
        internal void run(bool blocking)
        {
            String batchDir = StringUtils.getNewBatchId(true); // DateTime.Now.ToString("yyyyMMddHHmmss");
            RequestHandler.getInstance().BatchDirectory = this.Report.BatchId = batchDir; // set the batch directory at startup
            _report.addDebug("Set Orchestrator batch directory: " + batchDir);

            setWorkList();

            if (blocking)
            {
                startListenerAndMonitor();
            }
            else // start in seperate thread
            {
                BackgroundWorker bw = new BackgroundWorker();
                bw.DoWork += startListenerAndMonitor;
                bw.RunWorkerCompleted += bwCompleted;
                bw.RunWorkerAsync();
            }
            _report.addDebug("The orchestrator service has successfully shut down!");
        }

        /// <summary>
        /// Run the orchestator service in a blocking mode. Will not return until the work stack has been emptied and all
        /// client extractions have successfully completed
        /// </summary>
        protected override void run()
        {
            run(true);
        }

        public bool startListener()
        {
            // start up server and wait for connections
            try
            {
                _report.addDebug("Trying to start the orchestrator's listener...");
                _server = new Server();
                
                RequestHandler.getInstance().Extractors = _extractors;
                RequestHandler.getInstance().WorkStack = _workStack;
                RequestHandler.getInstance().ActiveExtractions = _activeExtractions;
                RequestHandler.getInstance().CompletedExtractions = _completedExtractions;
                RequestHandler.getInstance().ErroredExtractions = _erroredExtractions;

                RequestHandler.getInstance().ServiceState = _serviceState;

                //_server = server;
                // use the BackgroundWorker to help catch unhandled server exceptions
                BackgroundWorker bw = new BackgroundWorker();
                bw.DoWork += _server.startListener;
                bw.RunWorkerCompleted += bwCompleted;
                bw.RunWorkerAsync();

                //server.startListener();
                _report.addDebug("Successfully started the orchestrator's listener!");
                return true;
            }
            catch (Exception exc)
            {
                _report.addError(exc.Message, exc);
                _report.HasError = "T";
                return false;
            }

        }

        void bwCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled)
            {
                // not implemented
            }
            else if (e.Error != null)
            {
                // handle this exception
                _report.addError("An unexpected message was generated by the server!", e.Error);
                throw e.Error; // this should throw the same exception but on the calling thread
            }
            else if (e.Result != null)
            {
                // not implemented
            }
        }

        void startListenerAndMonitor(object sender, DoWorkEventArgs e)
        {
            startListenerAndMonitor();
        }

        void startListenerAndMonitor(object workStack)
        {
            _workStack = (ThreadSafeWorkStack)workStack;
            startListenerAndMonitor();
        }

        void startListenerAndMonitor()
        {
            if (!startListener())
            {
                throw new ApplicationException("Unable to start orchestrator listener! Unable to continue...");
            }

            _report.addInfo("The orchestrator service has started!");

            while (true)
            {
                try
                {
                    _report.addDebug("The main orchestrator process will sleep before checking it's job stack again");
                    System.Threading.Thread.Sleep(new TimeSpan(0, _cronSchedule, 0)); 

                    // first check if any of our workstacks have items - if not, see if db says we have work
                    if (_workStack.Count() == 0 && _activeExtractions.Count() == 0 && _erroredExtractions.Count() == 0 && _completedExtractions.Count() == 0)
                    {
                        getConfigs();
                        if (_configurations.Count == 0)
                        {
                            _report.addDebug("Didn't find any scheduled jobs for this iteration");
                            continue; // nothing on any of the collections and no jobs reported by getConfigs - SLEEP!
                        }
                        else // found configs! let's set the worklist
                        {
                            _report.addDebug("Found jobs scheduled for this iteration! Setting worklists and new batch ID");
                            setBatchIdAndWorklist();
                        }
                    }
                    //System.Threading.Thread.Sleep(sleepUntil.Subtract(curTime).Duration());
                    //sleepUntil = DateTime.Now.AddMinutes(_cronSchedule);
                    //_serviceState.NextRun = DateTime.Now.AddMinutes(_cronSchedule);
                    _report.addDebug(String.Format("The orchestrator service has completed an iteration - one configured every {0} minutes", _cronSchedule.ToString()));

                    _report.addInfo("The orchestrator reported " + _workStack.Count() + " jobs still on the stack at iteration");
                    _report.addInfo("The orchestrator reported " + _completedExtractions.Count() + " completed jobs at iteration");
                    _report.addInfo("The orchestrator reported " + _erroredExtractions.Count() + " errored jobs at iteration");
                    _report.addInfo("The orchestrator reported " + _activeExtractions.Count() + " active jobs at iteration");

                    if (_activeExtractions.Count() > 0 || _workStack.Count() > 0) // if there is still work occurring, don't clear anything - wait until jobs finish
                    {
                        _report.addDebug("Found active extractions/jobs on workstack - going back to sleep");
                        continue;
                    }

                    if (_completedExtractions.Count() > 0 || _erroredExtractions.Count() > 0) // only save our report if something interesting happened
                    {
                        _report.addInfo(String.Format("Looks like all jobs finished for batch {0}! - getting ready to save report", _report.BatchId));
                        _report.EndTimestamp = DateTime.Now;
                        try
                        {
                            _sqlDao.saveReport(_report);
                            logging.Log.LOG("Successfully saved report to db");
                        }
                        catch (Exception exc) 
                        {
                            logging.Log.LOG("There was a problem saving the report to SQL: " + exc.ToString());
                        }
                    }

                    sleepForDashboardIfTime();

                    _completedExtractions.RemoveAll();
                    _erroredExtractions.RemoveAll();

                    setBatchIdAndWorklist();

                    //if (_workStack.Count() == 0) // if we didn't find any jobs to add for this iteration, just continue so we sleep again
                    //{
                    //    _report.addDebug("Didn't find any jobs to add for this iteration");
                    //    continue;
                    //}

                    // TBD - do we really want to do this?
                    // ensure server is still listening - will throw an exception and be caught below if failure occurs
                    //Client c = new Client(_myIp.ToString(), _listeningPort);
                    //ThreadUtils.retryAction(new Action(() => c.connect()), 3, new TimeSpan(0, 1, 0), new ApplicationException("Unable to re-start listener after 10 attempts. Unable to continue..."));
                    //c.disconnect();
                    //_report.addDebug("Successfully verified the orchestrator service is listening for requests");
                }
                catch (Exception exc)
                {
                    _server.SocketContainer.Locked = true;
                    //_sqlThread.Abort();
                    _report.addError(exc.Message, exc);
                    ((OrchestratorReport)_report).HasError = "T";
                    _sqlDao.saveReport(_report);
                    throw exc;
                }
            }
        }

        void sleepForDashboardIfTime()
        {
            if (_dashboardCleanupTime <= 0)
            {
                return;
            }
            if (DateTime.Now.AddMinutes(_dashboardCleanupTime).CompareTo(_serviceState.NextRun) >= 0) // if the dashboard cleanup sleep time would take us past the next scheduled run, don't bother sleeping
            {
                return;
            }
            System.Threading.Thread.Sleep(new TimeSpan(0, _dashboardCleanupTime, 0));
        }

        void setBatchIdAndWorklist()
        {
            // dont allow the processing of new jobs when the work lists are being modified
            _server.SocketContainer.Locked = RequestHandler.getInstance().Locked = true;

            String batchDir = StringUtils.getNewBatchId(true); // DateTime.Now.ToString("yyyyMMddHHmmss");
            _report = new OrchestratorReport(batchDir) { OrchestratorHostName = _myHostName };
            RequestHandler.getInstance().BatchDirectory = _report.BatchId = batchDir; // set the batch directory at each iteration
            _report.addDebug("Starting new iteration");
            _report.addDebug("Set Orchestrator batch directory: " + batchDir);
            setWorkList();

            _server.SocketContainer.Locked = RequestHandler.getInstance().Locked = false;
        }
        void refreshConfigFileSettings()
        {
            ConfigurationManager.RefreshSection("appSettings"); // refresh this on every iteration so we don't have to bring down process to update config file
            Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.CronSchedule], out _cronSchedule);
            
            // Determine what type of SQL database we are using
            String sqlProvider = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider];
            String connectionString = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString];
            _sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(sqlProvider, connectionString));

            // make own unique list of sitecodes
            VistaDao vistaDao = new VistaDao();
            string[] sitecode = new string[vistaDao.SiteCodes.Count];
            vistaDao.SiteCodes.CopyTo(sitecode, 0);
            _vhaSites = new List<string>(sitecode);

            if (_vhaSites == null || _vhaSites.Count == 0)
            {
                _report.addException(new ApplicationException("Failed to open the VHA site file!"));
                throw new ApplicationException("Failed to open the VHA site file!");
            }
        }

        void reportHandler(Int32 sleepMinutes)
        {
            if (_activeExtractions.Count() == 0 && _workStack.Count() == 0 && _completedExtractions.Count() > 0)
            {
                try
                {
                    _sqlDao.saveReport(_report);
                }
                catch (Exception) { /* worried about this? */ }
            }
            _activeExtractions.RemoveAll();
            _completedExtractions.RemoveAll();
        }

        internal override void shutdown()
        {
            _report.addDebug("The orchestrator is getting ready to shut down!");
            _serviceState.Status = ServiceStatus.STOPPED;
            // first shut down our listener
            try
            {
                Client c = new Client();
                c.connect(_myIp.ToString(), _listeningPort);
                if (!c.sendStopServerRequest("Requesting the orchestrator service shut down for end of day clean up"))
                {
                    throw new ApplicationException("The server responded negatively to the shutdown request!");
                }
                _report.addDebug("The orchestrator server ackowledged the shutdown request!");
                System.Threading.Thread.Sleep(5000); // give this 5 seconds to complete
                // make sure the server has shut down
                try
                {
                    c.connect(_myIp.ToString(), _listeningPort);
                    c.disconnect();
                    throw new ApplicationException("The server responded successfully to the shutdown request but we can still connect!");
                }
                catch (Exception) { /* want an exception here!! */ }

                _report.addDebug("Successfully confirmed server shutdown!");
            }
            catch (Exception exc)
            {
                _report.addError("The orchestrator service is not responding properly to a shutdown request. It will need to be manually stopped!", exc);
                _report.HasError = "T";
                throw;
            }

            // then stop any running jobs
            if (_extractors != null && _extractors.Count() > 0)
            {
                IList<Extractor> activeExtractors = _extractors.GetExtractors();
                _report.addDebug("Found " + activeExtractors.Count + " active extractors - attempting to shut them down...");
                foreach (Extractor e in activeExtractors)
                {
                    shutdownClient(e.HostName, e.ListeningPort, 3);
                    try
                    {
                        // the stop extractor request should unlock the site but we'll do it here again to make sure
                        _sqlDao.unlockSite(e.SiteCode, e.VistaFile);
                    }
                    catch (Exception exc)
                    {
                        _report.addError("Trying end of day clean up - unable to unlock an extractor!", exc);
                        _report.HasError = "T";
                    }
                }
            }
        }

        public void shutdownClient(string hostname, Int32 listeningPort, Int32 maxTries)
        {
            int attempts = 0;
            while (attempts < maxTries)
            {
                attempts++;
                try
                {
                    Client c = new Client();
                    c.connect(hostname, listeningPort);
                    if (!c.sendStopServerRequest("End of the day!"))
                    {
                        System.Threading.Thread.Sleep(2500); // wait a couple seconds and try again
                        continue;
                    }
                    else
                    {
                        _report.addDebug("Successfully shut down " + hostname + ":" + listeningPort);
                        return; // all done with shut down!
                    }
                }
                catch (Exception exc)
                {
                    _report.addError(exc.Message, exc);
                    _report.HasError = "T";
                    return;
                }
            }
        }

        /// <summary>
        /// Prune the list of database configurations based on their CRON
        /// </summary>
        /// <param name="dbConfigs">The configurations as present in the database</param>
        /// <returns>The configs pruned by CRON</returns>
        private IList<ExtractorConfiguration> pruneConfigByCRON(IList<ExtractorConfiguration> dbConfigs)
        {
            // Set up
            DateTime start = DateTime.Now;
            DateTime end = start.AddMinutes(_cronSchedule);
            IList<ExtractorConfiguration> configs = new List<ExtractorConfiguration>();

            IList<String> distinctCron = new List<String>();

            // For each job with a cron defined, if it occurrs, add it to the stack
            foreach (ExtractorConfiguration config in dbConfigs)
            {
                if (String.IsNullOrEmpty(config.CRON))
                {
                    _report.addInfo("Skipping config: " + config.QueryConfigurations.RootNode.Value.File + " due to no CRON defined");
                }
                else
                {
                    if (!distinctCron.Contains(config.CRON))
                    {
                        distinctCron.Add(config.CRON); // TODO - use this to determine next run
                    }
                    try
                    {
                        if (CrontabSchedule.Parse(config.CRON).GetNextOccurrences(start, end).Count() > 0)
                        {
                            configs.Add(config);
                        }
                    }
                    catch (Exception exc)
                    {
                        _report.addError("Exception building schedule for config:" + config.CRON + ":" + exc.Message, exc);
                        _report.HasError = "T";
                    }
                }
            }

            // All done!
            setNextRunFromDistinctCron(distinctCron);
            return configs;
        }

        void setNextRunFromDistinctCron(IList<String> distinctCronStrings)
        {
            DateTime earliest = DateTime.Now.AddDays(1);
            if (distinctCronStrings != null && distinctCronStrings.Count > 0)
            {
                foreach (String cron in distinctCronStrings)
                {
                    DateTime nextRun = CrontabSchedule.Parse(cron).GetNextOccurrence(DateTime.Now.AddMinutes(_cronSchedule));

                    if (nextRun.CompareTo(earliest) < 0)
                    {
                        earliest = nextRun;
                    }
                }
            }
            _serviceState.NextRun = earliest;
        }

        DateTime _lastFetchedDbConfigs = DateTime.Now;
        public IList<ExtractorConfiguration> getConfigs()
        {
            _report.addDebug("The orchestrator is attempting to obtain it's configuration data");

            if (_configurations != null && 
                DateTime.Now.Subtract(_lastFetchedDbConfigs).TotalMinutes < 5) // don't let this happen too often - this smells but need to refactor out some of the spaghetti...
            {
                _report.addDebug("Tried fetching configs from database twice in < 5 minutes - using saved configs");
                return _configurations;
            }
            try
            {
                _configurations = pruneConfigByCRON(_sqlDao.getActiveExtractorConfigurations());
                _lastFetchedDbConfigs = DateTime.Now;

                // compute the next run time based off config and sleep configuration param
                if (_configurations != null && _configurations.Count > 0)
                {
                    try
                    {
                        _serviceState.NextRun = CrontabSchedule.Parse(_configurations[0].CRON).GetNextOccurrence(DateTime.Now.AddMinutes(_cronSchedule));
                    }
                    catch (Exception exc)
                    {
                        _report.addError("Problem computing next run time: " + exc.Message);
                    }
                }

                // get the set of orchestrator host names so we can provide this information to the extractors for failover
                IList<string> orchestrators = _sqlDao.getOrchestrators();
                // only add an orchestrator host name if it is not the current host name
                foreach (ExtractorConfiguration config in _configurations)
                {
                    config.AllOrchestrators = new List<string>();
                    foreach (string s in orchestrators)
                    {
                        if (!String.Equals(s, _myHostName, StringComparison.CurrentCultureIgnoreCase))
                        {
                            config.AllOrchestrators.Add(s);
                        }
                    }
                }

                adjustConfigs(_configurations);

                _activeJobsAtStartUp = _sqlDao.getLockedFiles();
                if (_activeJobsAtStartUp == null) // can be empty but not null - something probably went wrong
                {
                    throw new ApplicationException("The list of active jobs appears to be malformed");
                }
                Log.LOG("The SQL database reported " + _activeJobsAtStartUp.Count + " active jobs at iteration");
                _report.addInfo("The SQL database reported " + _activeJobsAtStartUp.Count + " active jobs at iteration");
                _report.addDebug("The SQL database reported " + _activeJobsAtStartUp.Count + " active jobs to the orchestrator on iteration");
            }
            catch (Exception exc)
            {
                Log.LOG("Unable to start orchestrator because no configurations were found: " + exc.ToString());
                _report.addError("Unable to start orchestrator because no configurations were found", exc);
                _report.HasError = "T";
                throw exc;
            }
            _report.addDebug("The orchestrator service found " + _configurations.Count + " extraction configurations");
            return _configurations;
        }

        String _file69From = "3100101";

        internal void adjustConfigs(IList<ExtractorConfiguration> configs)
        {
            Dictionary<String, Int32> overlapsByFile = _sqlDao.getCurrentOverlapsByFile();

            foreach (String key in overlapsByFile.Keys)
            {
                foreach (ExtractorConfiguration config in configs)
                {
                    TreeNode<QueryConfiguration> searchRslt = config.QueryConfigurations.search(new QueryConfiguration() { File = key });
                    if (searchRslt != null)
                    {
                        searchRslt.Value.applyDateOverlaps(overlapsByFile[key], 1);
                        logging.Log.LOG(String.Format("Adjusted config {0} - set overlap to {1}", key, overlapsByFile[key]));
                        if (String.Equals(key, "69"))
                        {
                            logging.Log.LOG("Set static file 69 FROM value");
                            _file69From = searchRslt.Value.From;
                        }
                        // don't break - we may have multiple configs for a file
                    }
                }
            }
        }

        /// <summary>
        /// Pre-process is responsible for doing anything that needs to happen for a job before it is
        /// put on the stack for work
        /// </summary>
        /// <param name="configs"></param>
        private void preProcess(IList<ExtractorConfiguration> configs)
        {
            return; // actually... don't care what type of SQL DAO - just return from all of them 

            // for each extractor configuration, perform the on_start sql
            foreach (ExtractorConfiguration config in configs)
            {
                ISqlDao sqlDao = _sqlDao;
                try
                {
                    if (config.ON_START == null || config.ON_START.Equals(String.Empty)) { }
                    else
                    {
                        try
                        {
                            sqlDao.executeDelimited(config.ON_START, 0);
                        }
                        catch (Exception exc)
                        {
                            _report.addError("An error occurred during the execution of on_start:" + config.QueryConfigurations.RootNode.Value.File, exc);
                        }
                    }

                    // for each query configuration, disable indexes on the file
                    IList<string> distinctFiles = new List<string>();
                    parseDistinctFiles(config.QueryConfigurations.RootNode, distinctFiles);
                    foreach (string file in distinctFiles)
                    {
                        try
                        {
                            sqlDao.disableIndexes(file);
                        }
                        catch (Exception exc)
                        {
                            _report.addError("An error occurred during the de-indexing of file:" + file, exc);
                        }
                    }
                    _sqlDao.executeStoredProcedureNoArguments(config.QueryConfigurations.RootNode.Value.File + "_BEGIN", 0);
                }
                catch (Exception exc)
                {
                    if (exc.Message.Contains("identifier '" + config.QueryConfigurations.RootNode.Value.File + "_BEGIN' must be declared"))
                    {
                        // Ignore exceptions related to the procedure not being defined
                        // TODO: not sql server safe
                    }
                    else
                    {
                        Report.addError(exc.Message, exc);
                        ((OrchestratorReport)Report).HasError = "T";
                    }
                }
            }
        }

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

        public ThreadSafeWorkStack setWorkList()
        {
            _allConfigs = new ThreadSafeWorkStack();
            getConfigs();

            // cleanup from last batch - delete IEN tracking table records since IEN would be counted otherwise
            cleanupFromLastRun();

            ISqlDao sqlDao = _sqlDao;
            Dictionary<String, String> lastIenTable = _sqlDao.getLastIenTable(); // grab the whole table - much better performance than a query per config/per site

            // loop through sites and jobs - see if job was already reported as started by database
            foreach (ExtractorConfiguration ec in _configurations)
            {
                QueryConfiguration qc = ec.QueryConfigurations.RootNode.Value;
                foreach (string sitecode in _vhaSites)
                {
                    ec.SiteCode = sitecode;
                    string currentKey = sitecode + "_" + qc.File;

                    if (!String.IsNullOrEmpty(ec.Sites))
                    {
                        bool included = false;
                        string[] configSites = ec.Sites.Split(new char[] { ';' });
                        if (configSites != null && configSites.Length > 0)
                        {
                            foreach (string s in configSites)
                            {
                                if (sitecode == s)
                                {
                                    ExtractorConfiguration cloned = ec.Clone();
                                    cloned.BatchId = this.Report.BatchId;
                                    _allConfigs.Push(cloned);
                                    included = true;
                                }
                            }
                        }
                        if (!included)
                        {
                            continue;
                        }
                    }
                    else
                    {
                        ExtractorConfiguration cloned = ec.Clone();
                        cloned.BatchId = this.Report.BatchId;
                        _allConfigs.Push(cloned);
                    }

                    // we will give the start point to the extractor for incremental extractions so we need to fetch it
                    ExtractorConfiguration newConfig = ec.Clone();
                    newConfig.BatchId = this.Report.BatchId;
                    if (ec.ExtractMode == ExtractorMode.INCREMENTAL)
                    {
                        try
                        {
                            String lastSqlIen = "0";

                            if (lastIenTable.ContainsKey(currentKey))
                            {
                                lastSqlIen = lastIenTable[currentKey]; // if we found this config/site key, use last IEN from last IEN table
                                _report.addDebug("Found incremental IEN in tracking table for key: " + currentKey + " - IEN: " + lastSqlIen);
                            }
                            //string lastSqlIen = _sqlDao.getLastIen(sitecode, config.File);
                            string fromConfig = newConfig.QueryConfigurations.RootNode.Value.From;
                            if (lastSqlIen.Equals("0") && !String.IsNullOrEmpty(fromConfig))
                            {
                                newConfig.StartIen = fromConfig;
                            }
                            else
                            {
                                if (String.IsNullOrEmpty(fromConfig)) // fromConfig == null || fromConfig.Equals(String.Empty))
                                {
                                    newConfig.StartIen = lastSqlIen;
                                }
                                else
                                {
                                    decimal incIen = Convert.ToDecimal(lastSqlIen);
                                    decimal fromIen = Convert.ToDecimal(fromConfig);
                                    if (incIen > fromIen)
                                    {
                                        newConfig.StartIen = lastSqlIen;
                                        newConfig.QueryConfigurations.RootNode.Value.From = lastSqlIen;
                                    }
                                    else
                                    {
                                     //   _report.addInfo(String.Format("The current configuration specified a start IEN ({0}) that is greater than the last IEN in the tracking table ({1}) - Site: {2}, File: {3}", fromConfig, lastSqlIen, sitecode, qc.File));
                                        newConfig.StartIen = fromConfig;
                                        newConfig.QueryConfigurations.RootNode.Value.From = fromConfig;
                                    }
                                }
                            }
                            // do this at the end so we don't disturb any of the above code - don't care what's in IEN tracking table for these files, always start at beginning
                            if (String.Equals(newConfig.QueryConfigurations.RootNode.Value.File, "63") || 
                                String.Equals(newConfig.QueryConfigurations.RootNode.Value.File, "55")) // not crazy about this hard coding but at least it's just the Orchestrator handling it...
                            {
                             //   logging.Log.LOG("Found a special config that requires a full Vista file traversal - setting start IEN to '0'");
                             //   _report.addInfo("Found a special config that requires a full Vista file traversal - setting start IEN to '0'");
                                newConfig.StartIen = newConfig.QueryConfigurations.RootNode.Value.From = "0";
                            }
                            if (String.Equals(newConfig.QueryConfigurations.RootNode.Value.File, "69"))
                            {
                              //  logging.Log.LOG("Updated file 69 config - set config start IEN to " + _file69From);
                                newConfig.StartIen = newConfig.QueryConfigurations.RootNode.Value.From = _file69From;
                            }
                        }
                        catch (Exception exc)
                        {
                            _report.addError("Unable to retrieve the last SQL IEN for the extractor!", exc);
                            _report.HasError = "T";
                            continue;
                        }
                    }
                    else if (ec.ExtractMode == ExtractorMode.DIFF)
                    {
                        newConfig.SqlIens = setParamsForDiff(sitecode, ec);
                    }

                    _report.addDebug("Adding a new job to the workstack: " + newConfig.ToString());
                    _workStack.PushOrUpdate(newConfig);
                }
            }
            _report.addInfo(_workStack.Count() + " total jobs are on the work stack");
            _report.addDebug(_workStack.Count() + " total jobs on the work stack");

            preProcess(_configurations);

            _workStack.SortBySiteCode(); // we want jobs for a site to run as a group
            RequestHandler.getInstance().WorkStack = _workStack;
            return _workStack;
        }

        private void cleanupFromLastRun()
        {
            _report.addInfo("Cleaning up db IEN tracking table for failed sites from last batch");
            logging.Log.LOG("Cleaning up db IEN tracking table for failed sites from last batch");
            _sqlDao.rollbackIenTrackingForFailedSites();
            _report.addInfo("Completed cleanup of db for failed sites from last batch");
            logging.Log.LOG("Completed cleanup of db for failed sites from last batch");
        }

        private IList<String> setParamsForDiff(string sitecode, ExtractorConfiguration ec)
        {
            IList<String> iensFromSql = _sqlDao.getPatientIensForSite(sitecode);
            return iensFromSql;
        }

        public bool tryClientConnect(Extractor job, int attempts, bool unlockOnFail)
        {
            Client c = new Client();
            _report.addDebug("Attempting to connect to a client to verify a task is still running...");
            for (int i = 0; i < attempts; i++)
            {
                try
                {
                    c.connect(job.HostName, job.ListeningPort);
                    c.disconnect();
                    _report.addDebug("The client appears to still be running!");
                    return true;
                }
                catch (Exception)
                {
                    c.disconnect();
                    if (i < attempts)
                    {
                        _report.addDebug("The client did not respond on attempt number " + (i + 1) + ", will try " + (attempts - i - 1) + " more times");
                        System.Threading.Thread.Sleep(1000);
                        continue;
                    }
                    if (unlockOnFail)
                    {
                        try
                        {
                            _report.addDebug("The client did not respond after " + attempts + " connection attempts. Unlocking the job");
                            _sqlDao.unlockSite(job.SiteCode, job.VistaFile);
                        }
                        catch (Exception)
                        {
                            // this is probably ok - should get cleaned up eventually
                        }
                    }
                    return false;
                }
            }
            return false;
        }

        public override ServiceState getServiceState()
        {
            throw new NotImplementedException();
        }

        public override ServiceState setServiceState(ServiceStatus setTo)
        {
            throw new NotImplementedException();
        }
    }
}
