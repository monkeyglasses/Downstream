using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.dao.file;
using com.bitscopic.downstream.utils;
using System.IO;
using System.Configuration;
using com.bitscopic.downstream.domain;

namespace com.bitscopic.downstream.dao.vista
{
    public static class VistaDaoUtils
    {
        internal static IList<String> getIensFromVistaFile(IVistaDao dao, String siteId, String vistaFile)
        {
            //IVistaDao dao = new VistaDaoFactory().getVistaDao(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VistaDaoType]);
            String startIen = "0";
            IList<String> result = new List<String>();
            Char[] ddrDelim = new Char[] { '^' };
            Decimal greatestIen = 0;
            bool infiniteLoopFlag = false;

            while (!infiniteLoopFlag)
            {
                String[] results = dao.ddrLister(siteId, vistaFile, "", ".01", "IP", "5000", startIen, "", "#", "", "");
                if (results == null || results.Length == 0)
                {
                    break;
                }

                foreach (String s in results)
                {
                    String currentIen = s.Split(ddrDelim)[0];
                    Decimal currentIenAsDecimal = Convert.ToDecimal(currentIen);
                    if (currentIenAsDecimal <= greatestIen)
                    {
                        infiniteLoopFlag = true;
                        break;
                    }
                    greatestIen = currentIenAsDecimal;
                    result.Add(currentIen);
                }
                startIen = greatestIen.ToString();
            }

            return result;
        }

        internal static Dictionary<String, String> toDictFromDdrGetsEntry(String[] response)
        {
            Dictionary<String, String> result = new Dictionary<String, String>();

            if (response == null || response.Length <= 0)
            {
                return result;
            }

            for (int i = 0; i < response.Length; i++)
            {
                if (String.IsNullOrEmpty(response[i]))
                {
                    continue;
                }
                String[] pieces = response[i].Split(new char[] { '^' });
                if (pieces == null || pieces.Length < 3)
                {
                    continue;
                }
                result.Add(pieces[2], pieces[3]);
            }

            return result;
        }

        internal static String toStringFromDdrGetsEntryWP(String[] response)
        {
            StringBuilder sb = new StringBuilder();
            bool inTextPart = false;
            for (int i = 0; i < response.Length; i++)
            {
                if (!inTextPart && response[i].Contains("[WORD PROCESSING]"))
                {
                    inTextPart = true;
                    continue;
                }
                if (!inTextPart)
                {
                    continue;
                }

                if (response[i].Contains("$$END$$"))
                {
                    // remove the trailing newline characters added on the last run through the loop
                    foreach (char c in Environment.NewLine)
                    {
                        sb.Remove(sb.Length - 1, 1);
                    }
                    break;
                }

                sb.AppendLine(response[i]);
            }

            return sb.ToString();
        }

        /// <summary>
        /// Strip all delimiters being used in our delimited file format, RS and US by default, from results
        /// </summary>
        /// <param name="arg"></param>
        /// <returns></returns>
        public static String[] stripInvalidChars(String[] result)
        {
            // wish we didn't have to do this here but string does not appear to be encoded as expected until after deserialization
            if (result != null && result.Length > 0)
            {
                IList<Char> stripChars = DataTableUtils.getDelimiters();
                for (int i = 0; i < result.Length; i++)
                {
                    result[i] = StringUtils.stripChars(result[i], stripChars);
                }
            }
            return result;
        }

        static readonly object _labChemDataDictionaryLocker = new object();
        public static Dictionary<String, String> getLabChemFieldsDynamic(String sitecode, gov.va.medora.mdo.dao.AbstractConnection cxn)
        {
            lock (_labChemDataDictionaryLocker) // make this method thread safe so no race conditions
            {
                String fileName = "C:\\File63x04FieldNumbersAndNames" + sitecode + ".dat";
                if (File.Exists(fileName))
                {
                    Int16 defaultCacheDuration = 4;
                    Int16.TryParse(ConfigurationManager.AppSettings["LabChemDataDictionaryCacheDays"], out defaultCacheDuration);
                    logging.Log.LOG("File exists!!");
                    if (DateTime.Now.Subtract(new FileInfo(fileName).CreationTime).Duration().Days > defaultCacheDuration)
                    {
                        logging.Log.LOG("File > " + defaultCacheDuration.ToString() + " days old - deleting");
                        File.Delete(fileName);
                    }
                    else
                    {
                        logging.Log.LOG("File " + defaultCacheDuration.ToString() + " 4 days old - fetching cached!");
                        return getCachedLabChemFields(sitecode);
                    }
                }

                gov.va.medora.mdo.dao.vista.VistaFile file = new gov.va.medora.mdo.dao.vista.VistaFile() { FileNumber = "63.04" };
                gov.va.medora.mdo.dao.vista.VistaToolsDao dao = new gov.va.medora.mdo.dao.vista.VistaToolsDao(cxn);

                Dictionary<string, gov.va.medora.mdo.dao.vista.VistaField> result = dao.getFields(file);

                StringBuilder sb = new StringBuilder();
                foreach (String key in result.Keys)
                {
                    sb.Append(key);
                    sb.Append('\x1f');
                    sb.Append(result[key].VistaName);
                    sb.Append('\x1e');
                    //System.Console.WriteLine(key + " - " + result[key].VistaName);
                }
                using (FileStream fs = new FileStream(fileName, FileMode.Create))
                {
                    byte[] temp = Encoding.UTF8.GetBytes((String)sb.ToString());
                    fs.Write(temp, 0, temp.Length);

                    fs.Flush();
                    fs.Close();
                }
                return getCachedLabChemFields(sitecode);
            }
        }

        internal static Dictionary<String, String> getCachedLabChemFields(String siteId)
        {
            String contents = new FileDao(false).readFile("C:\\File63x04FieldNumbersAndNames" + siteId + ".dat");
            String[] lines = gov.va.medora.utils.StringUtils.split(contents, '\x1e');
            Dictionary<String, String> result = new Dictionary<string, string>();
            for (int i = 0; i < lines.Length; i++)
            {
                if (String.IsNullOrEmpty(lines[i]))
                {
                    continue;
                }

                String[] pieces = gov.va.medora.utils.StringUtils.split(lines[i], '\x1f');
                if (pieces.Length != 2)
                {
                    continue;
                }

                String key = pieces[0].Trim();
                String value = pieces[1].Trim();

                if (!result.ContainsKey(key))
                {
                    result.Add(key, value);
                }
            }

            return result;
        }


        /// <summary>
        /// Take a list of IENS strings and dynamically fetch the comments for each
        /// </summary>
        /// <param name="siteId"></param>
        /// <param name="iensWithComments">e.g. [ ",6899697.918861,277,", ",6899495.926255,277,", ",6899282.926372,277," ]</param>
        /// <returns>Dictionary by IENS populated only with records which contained comments</returns>
        internal static Dictionary<String, String> getCommentsForIens(String siteId, IList<String> iensWithComments)
        {
            Dictionary<String, String> result = new Dictionary<string, string>();

            for (int i = 0; i < iensWithComments.Count; i++)
            {
                String comment = getComment(siteId, iensWithComments[i]);
                if (!String.IsNullOrEmpty(comment))
                {
                    result.Add(iensWithComments[i], comment);
                }
            }

            return result;
        }

        internal static String getComment(String siteId, String iens)
        {
            try
            {
                String[] subResults = new VistaDaoFactory().getVistaDao(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VistaDaoType]).ddrLister(siteId, "63.041", iens, ".01", "IP", "", "", "", "#", "", "");

                if (subResults == null || subResults.Length == 0)
                {
                    return String.Empty;
                }

                char[] delim = new char[] { '^' };
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < subResults.Length; i++)
                {
                    sb.AppendLine(subResults[i].Split(delim)[1]);
                }

                return sb.ToString();
            }
            catch (Exception)  { }

            return String.Empty;
        }

        /// <summary>
        /// Uses the SCREEN param and field .16 to traverse the PATIENT file backwards searching for the last record created after the specified date
        /// </summary>
        /// <param name="siteId"></param>
        /// <returns></returns>
        internal static String getLastPatientIenByDate(String siteId)
        {
            return getLastPatientIenByDate(siteId, "");
        }

        /// <summary>
        /// Uses the SCREEN param and field .16 to traverse the PATIENT file backwards searching for the last record created after the specified date
        /// </summary>
        /// <param name="siteId"></param>
        /// <param name="vistaDate">Defaults to '3131031' - the date the IEN problem was first discovered</param>
        /// <returns></returns>
        internal static String getLastPatientIenByDate(String siteId, String vistaDate)
        {
            String result = new VistaDaoFactory().getVistaDao(
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VistaDaoType])
                .getVariableValueQuery(siteId, "$G(^DIA(2,$O(^DIA(2,\"A\"),-1),0))");

            String lastIen = gov.va.medora.utils.StringUtils.split(result, gov.va.medora.utils.StringUtils.CARET)[0];

            Int64 trash = 0;
            if (String.IsNullOrEmpty(result) || Int64.TryParse(lastIen, out trash))
            {
                throw new downstream.domain.exception.LastPatientIenFromAuditException();
            }

            return lastIen;
        }
    }
}
