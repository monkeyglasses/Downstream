using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;
using System.Data;
using System.Configuration;

namespace com.bitscopic.downstream.utils
{
    public static class QueryResultsUtils
    {
        public static QueryResults cleanupTicket85(QueryResults qr)
        {
            if (findTableInQueryResults(qr, "63.05") != null)
            {
                fixLabTablesTicket85(qr);
            }
            return qr;
        }

        public static QueryResults cleanupTicket76(QueryResults qr, IList<String> iens = null)
        {
            if (String.Equals(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.TrimTopLevelIens], "false", StringComparison.CurrentCultureIgnoreCase))
            {
                return qr;
            }

            if (findTableInQueryResults(qr, "55") != null)
            {
                clean55DataTable(qr);
            }
            else if (findTableInQueryResults(qr, "63") != null)
            {
                clean63DataTable(qr, iens);
            }
            return qr;
        }

        internal static void clean63DataTable(QueryResults qr, IList<String> iens63x04 = null)
        {
            DataTable table63 = qr.DdrResults[0];
            DataTable table63x05 = findTableInQueryResults(qr, "63.05");
            Dictionary<String, object[]> finalTableDict = new Dictionary<String, object[]>();

            Dictionary<String, object[]> beforeTableDict = new Dictionary<String, object[]>();
            for (int i = 0; i < table63.Rows.Count; i++)
            {
                String topLevelIen = (String)table63.Rows[i]["IEN"];
                if (!beforeTableDict.ContainsKey(topLevelIen))
                {
                    beforeTableDict.Add(topLevelIen, table63.Rows[i].ItemArray);
                }
            }

            if (table63x05 != null)
            {
                for (int j = 0; j < table63x05.Rows.Count; j++)
                {
                    String ienFrom63x05 = (String)table63x05.Rows[j]["P_IEN"]; // get parent IEN
                    if (!finalTableDict.ContainsKey(ienFrom63x05)) // make sure we check for IEN first
                    {
                        finalTableDict.Add(ienFrom63x05, beforeTableDict[ienFrom63x05]);
                    }
                }
            }

            if (iens63x04 != null && iens63x04.Count > 0) // these would have come from 63.04 - need to check for these now, too
            {
                foreach (String ien in iens63x04)
                {
                    if (!finalTableDict.ContainsKey(ien))
                    {
                        finalTableDict.Add(ien, beforeTableDict[ien]);
                    }
                }
            }

            table63.Clear();
            foreach (object[] row in finalTableDict.Values)
            {
                table63.Rows.Add(row);
            }
        }


        internal static void clean55DataTable(QueryResults qr)
        {
            DataTable table55 = qr.DdrResults[0];
            DataTable table55x01 = findTableInQueryResults(qr, "55.01");
            DataTable table55x06 = findTableInQueryResults(qr, "55.06");

            Dictionary<String, object[]> finalTableDict = new Dictionary<String, object[]>();

            Dictionary<String, object[]> beforeTableDict = new Dictionary<String, object[]>();
            for (int i = 0; i < table55.Rows.Count; i++)
            {
                String topLevelIen = (String)table55.Rows[i]["IEN"];
                beforeTableDict.Add(topLevelIen, table55.Rows[i].ItemArray);
            }

            // go through table 55.01 first
            if (table55x01 != null)
            {
                for (int j = 0; j < table55x01.Rows.Count; j++)
                {
                    String ienFrom55x01 = (String)table55x01.Rows[j]["P_IEN"]; // get parent IEN
                    if (!finalTableDict.ContainsKey(ienFrom55x01)) // make sure we check for IEN first
                    {
                        finalTableDict.Add(ienFrom55x01, beforeTableDict[ienFrom55x01]);
                    }
                }
            }
            // go through table 55.06 next
            if (table55x06 != null)
            {
                for (int k = 0; k < table55x06.Rows.Count; k++)
                {
                    String ienFrom55x06 = (String)table55x06.Rows[k]["P_IEN"]; // get parent IEN
                    if (!finalTableDict.ContainsKey(ienFrom55x06)) // make sure we check for IEN first
                    {
                        finalTableDict.Add(ienFrom55x06, beforeTableDict[ienFrom55x06]);
                    }
                }
            }

            table55.Clear();
            foreach (object[] row in finalTableDict.Values)
            {
                table55.Rows.Add(row);
            }
        }

        internal static DataTable findTableInQueryResults(QueryResults qr, String tableName)
        {
            if (qr == null || qr.DdrResults == null || qr.DdrResults.Count == 0)
            {
                return null;
            }
            for (int i = 0; i < qr.DdrResults.Count; i++)
            {
                if (String.Equals(qr.DdrResults[i].TableName, tableName))
                {
                    return qr.DdrResults[i];
                }
            }
            return null;
        }

        internal static void fixLabTablesTicket85(QueryResults qr)
        {
            if (!String.Equals(qr.DdrResults[0].TableName, "63"))
            {
                return;
            }

            if (String.Equals(qr.DdrResults[0].TableName, "63"))
            {
                DataTable dt63x05 = findTableInQueryResults(qr, "63.05");
                if (dt63x05 == null)
                {
                    return;
                }
                else
                {
                    dt63x05 = stringReplaceInField(dt63x05, ".06", "&amp;", "&");
                    return;
                }
            }
        }

        internal static DataTable stringReplaceInField(DataTable table, String fieldNumberOrColumnName, String textToFind, String replacementText)
        {
            if (table.Rows.Count == 0 || !table.Columns.Contains(fieldNumberOrColumnName))
            {
                return table;
            }

            foreach (DataRow row in table.Rows)
            {
                String cellValue = row[fieldNumberOrColumnName] as String;
                cellValue = cellValue.Replace(textToFind, replacementText);
                row[fieldNumberOrColumnName] = cellValue;
            }

            return table;
        }

        public static bool checkTicket15(QueryResults qr, ExtractorConfiguration config)
        {
            if (!String.Equals("612", config.SiteCode) && !String.Equals("648", config.SiteCode) && !String.Equals("635", config.SiteCode)) // bug only encountered in 612 so far - check that site only! - found another case in files 200 and 55 in Portland!
            {
                return false;
            }
            if (!String.Equals("63", config.QueryConfigurations.RootNode.Value.File) &&
                !String.Equals("200", config.QueryConfigurations.RootNode.Value.File) &&
                !String.Equals("55", config.QueryConfigurations.RootNode.Value.File)) // next, bug only encountered in file 63 in sites 612 - check that file only! - found another case in files 200 and 55 in Portland!
            {
                return false;
            }
            bool foundRowWithZero = false;
            for (int i = qr.DdrResults[0].Rows.Count; i > 0; i--) // work backwards so as not to screw up indices
            {
                if (String.Equals(qr.DdrResults[0].Rows[i - 1][0] as String, "0"))
                {
                    foundRowWithZero = true;
                    qr.DdrResults[0].Rows.RemoveAt(i - 1); // remove all rows with "0" IEN
                }
            }

            if (foundRowWithZero && String.Equals("648", config.SiteCode) && String.Equals("200", config.QueryConfigurations.RootNode.Value.File)) // also found weird IENs in this file/site - let's remove them too while we're at it!
            {
                for (int i = qr.DdrResults[0].Rows.Count; i > 0; i--) // work backwards so as not to screw up indices
                {
                    if (String.Equals(qr.DdrResults[0].Rows[i - 1][0] as String, "648")) // why is this IEN in this bunch of records??? i have no idea... let's remove it though
                    {
                        foundRowWithZero = true;
                        qr.DdrResults[0].Rows.RemoveAt(i - 1); // remove all rows with "648" IEN
                    }
                }
            }

            if (foundRowWithZero)
            {
                //_report.addInfo("Found 0 IEN in results! This issue was reported in ticket #15 in Assembla. For now, treating this as a complete file traversal and signaling complete");
                return true;
            }
            return false;
            // TODO - see ticket 15 
            // return true if site = 612 and file = 63 and received goofy results illustrated in ticket
        }

        public static bool checkTicket81(QueryResults qr, ExtractorConfiguration config)
        {
            if (!(String.Equals(config.SiteCode, "556") && String.Equals(config.QueryConfigurations.RootNode.Value.File, "52")))
            {
                return false;
            }
            // verified we're looking at 556, file 52 - now look at IENs in results for out of place record
            // out of place IEN as of 6/17/2013: 8485460
            if (String.Equals(qr.DdrResults[0].Rows[qr.DdrResults[0].Rows.Count - 1]["IEN"], "8485460"))
            {
                qr.DdrResults[0].Rows.RemoveAt(qr.DdrResults[0].Rows.Count - 1); // remove this last row
                //_report.addDebug("Removed record from results - site 556, file 52. Per ticket #81");
                return true;
            }
            return false;
        }

        public static bool checkTicket73(QueryResults qr, ExtractorConfiguration config)
        {
            if (String.Equals("593", config.SiteCode) && String.Equals("68", config.QueryConfigurations.RootNode.Value.File)) // this is a small file so can return true the first time we see site 593, file 68
            {
                return true;
            }
            return false;
        }

    }
}
