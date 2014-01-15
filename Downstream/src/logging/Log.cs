using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.IO;

namespace com.bitscopic.downstream.logging
{
    public static class Log
    {
        private static string LOG_PATH = @"LOG.txt";

        public static void setLogPath(String logPath)
        {
            LOG_PATH = logPath;
        }

        public static void LOG(string message)
        {
            log(message);
        }

        internal static void logDDR(String message)
        {
            bool trash = false;
            if (!Boolean.TryParse(ConfigurationManager.AppSettings["LogDDR"], out trash)) // if string can't be parse, default to false/not logging
            {
                return;
            }
            if (!trash) // if was parsed but set to false, don't log of course
            {
                return;
            }
            log(message);
        }

        private static void log(string message)
        {
            try
            {
                if (!File.Exists(LOG_PATH))
                {
                    File.Create(LOG_PATH);
                }
                using (FileStream fs = new FileStream(LOG_PATH, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine();
                    sb.Append(DateTime.Now.ToString());
                    sb.Append(" - [" + System.Threading.Thread.CurrentThread.Name + "] >");
                    sb.Append(message);
                    fs.Write(System.Text.Encoding.ASCII.GetBytes(sb.ToString()), 0, sb.Length);
                }
            }
            catch (Exception)
            {
                //System.Console.WriteLine("Error!");
                // do nothing
            }
        }
    }
}
