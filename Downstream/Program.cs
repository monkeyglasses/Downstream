using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using gov.va.medora.downstream.service;
using System.Configuration;
using System.Threading;
using System.Net.Mail;
using gov.va.medora.downstream.net;
using gov.va.medora.downstream.domain.exception;
using gov.va.medora.downstream.dao.sql;
using System.Reflection;
using System.Net;
using gov.va.medora.downstream.domain;
using gov.va.medora.downstream.domain.reporting;
using gov.va.medora.downstream.dao.file;

namespace gov.va.medora.downstream
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application. Starts a type of service
        /// </summary>
        static void Main(string[] args)
        {
            Service svc = null;
            try
            {
                if (args != null && args.Length > 0)
                {
                    svc = new ServiceFactory().getService(args[0]);
                }
                else
                {
                    svc = new ServiceFactory().getService(ConfigurationManager.AppSettings["ServiceType"]);
                }

                if (svc is OrchestratorService)
                {
                    svc.run();
                    saveReport(((OrchestratorService)svc).Report);
                }
                else if (svc is VistaService)
                {
                    DayOfWeek startDay = DateTime.Now.DayOfWeek;
                    // if day of week has changed, return to launcher which should check for latest software version
                    while (serverHasWork() && DateTime.Now.DayOfWeek == startDay)
                    {
                        svc.run();
                        saveReportText(((VistaService)svc).Report);
                    }
                }

            }
            catch (Exception exc)
            {
                if (svc != null && svc is OrchestratorService)
                {
                    ((OrchestratorService)svc).Report.addException(exc);
                    ((OrchestratorService)svc).Report.EndTimestamp = DateTime.Now;
                    saveReport(((OrchestratorService)svc).Report); // save the report if orchestrator errors for unknown reason
                }
                if (svc != null && svc is VistaService)
                {
                    saveReportText(((VistaService)svc).Report);
                }
            }

            return;
        }

        static void saveReportText(Report report)
        {
            bool logLocal = false;
            Boolean.TryParse(ConfigurationManager.AppSettings["LocalLogging"], out logLocal);
            if (logLocal)
            {
                logging.Log.LOG(report.ToString());
            }
        }

        static void saveReport(Report report)
        {
            try
            {
                new SqlDao().saveReport(report);
            }
            catch (Exception) { /* nothing to do here */ }
        }

        /// <summary>
        /// Ask server if work is available. If not, wait 1/2 hour and ask again
        /// </summary>
        /// <returns></returns>
        static bool serverHasWork()
        {
            Client c = new Client();
            while (true)
            {
                try
                {
                    c.connect(ConfigurationManager.AppSettings["OrchestratorHostName"],
                        Convert.ToInt32(ConfigurationManager.AppSettings["OrchestratorListeningPort"]));
                    // TODO - need a way to verify server has some work to be done! 
                    bool serverHasWork = c.sendServerHasWorkRequest();
                    c.disconnect();
                    if (serverHasWork)
                    {
                        return true;
                    }
                    else
                    {
                        //LOG.Debug("No work! Sleeping for 1/2 hour");
                        System.Threading.Thread.Sleep(1800000); // no work! wait 1/2 hour and ask again
                    }
                }
                catch (Exception)
                {
                    //LOG.Debug("Couldn't ask about available work! Sleeping for 1/2 hour");
                    System.Threading.Thread.Sleep(1800000); // server wasn't listening for connections
                    continue;
                }
            }
        }
    }
}
