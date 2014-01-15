using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.config
{
    public static class AppConfigSettingsConstants
    {
        public static String OrchestratorHostName = "OrchestratorHostName";

        public static String OrchestratorListeningPort = "OrchestratorListeningPort";

        public static String SqlConnectionString = "SqlConnectionString";

        public static String DownstreamFilePath = "DownstreamFilePath";

        public static String NetworkUserDomain = "NetworkUserDomain";

        public static String NetworkUserName = "NetworkUserName";

        public static String NetworkUserPassword = "NetworkUserPassword";

        public static String ServerHostName = "ServerHostName"; // use this to configure the hostname for the server

        public static String ServerPort = "ServerListeningPort"; // use this to configure the listening port for the services - be careful if using multiple threads!

        public static String SqlProvider = "SqlProvider";

        public static String QuerySvcURL = "QuerySvcURL";

        public static String FileDaoType = "FileDaoType"; // valid values: 

        public static String SubQueryWorkers = "SubQueryWorkers";

        public static String IncrementalFileUploads = "IncrementalExtractorFileUploads";

        public static String VistaDaoType = "VistaDaoType"; // valid values: MdoVistaDao, WcfVistaDao

        public static String WorkerThreads = "WorkerThreads"; // configure the number of worker threads for VistaService

        public static String ExtractorSleepTime = "ExtractorSleepTime"; // configure the time to sleep between checking if a worker thread is finished

        public static String CronSchedule = "CronSchedule"; // configure the sleep time for CRON scheduler

        public static String PackageMappingConfig = "PackageMappingConfigFilePath";

        public static String VhaSitesFilePath = "VhaSitesFilePath";

        public static String SeedStartDate = "SeedStartDate";

        public static String SqlDeleteFileOnUpload = "SqlDeleteFileOnUpload";

        public static String IncludeSubfileLogs = "LogSubfileMessages";

        public static String SeedOverlapDaysLabMicro = "SeedOverlapDaysLabMicro";

        public static String SeedOverlapDaysPharm55 = "SeedOverlapDaysPharmacy55";

        public static String GroupID = "GroupID";

        public static String TriggerInternalInformaticaPath = "InformaticaTriggerPath";

        public static String InformaticaArchivePathWithDriveLetter = "InformaticaArchivePathWithDriveLetter";

        public static String InformaticaArchivePathUnix = "InformaticaArchivePathUnix";

        public static String MaxRunTime = "MaxRunTime";

        public static String TrimTopLevelIens = "TrimTopLevelIens";

        public static String DiffModeSqlStringPatientIens = "DiffModeSqlStringPatientIens";

        public static String DiffModeSqlCxnString = "DiffModeSqlCxnString";

        public static String EnforceCallback = "EnforceCallback";

        public static String EtlMapCxnString = "EtlMapCxnString";

        public static String TriggerHeader = "TriggerHeader";

        public static String TriggerFlags = "TriggerFlags";

        public static String ExtractorMgrListeningPort = "ExtractorMgrListeningPort";

        public static String DashboardCleanupTime = "DashboardCleanupTime";
    }
}
