using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using com.bitscopic.downstream.dao.file;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using System.Collections.Concurrent;
using System.Configuration;
using com.bitscopic.downstream.domain.exception;

namespace com.bitscopic.downstream.utils
{
    public static class DataTableUtils
    {
        static String RECORD_SEPARATOR = String.IsNullOrEmpty(ConfigurationManager.AppSettings["RecordDelimiter"]) ? "\x1e" : ConfigurationManager.AppSettings["RecordDelimiter"];
        static String FIELD_SEPARATOR = String.IsNullOrEmpty(ConfigurationManager.AppSettings["FieldDelimiter"]) ? "\x1f" : ConfigurationManager.AppSettings["FieldDelimiter"];

        static Char[] semicolonDelim = new Char[1] { ';' };
        static Char[] caratDelim = new Char[1] { '^' };

        /// <summary>
        /// Generate a basic data table to store the results of a basic VistaQuery
        /// using a unified format
        /// </summary>
        /// <param name="tableName">The name of the table being created</param>
        /// <param name="fields">The fields the query will return</param>
        /// <returns></returns>
        internal static DataTable generateVistaQueryDataTable(string tableName, string[] fieldArray, bool subfile, string[] wpOrComputedFields)
        {
            DataTable retVal = new DataTable();
            retVal.TableName = tableName;
            if (subfile)
            {
                retVal.Columns.Add("P_IEN", typeof(string));
                retVal.Columns.Add("IEN", typeof(string));
                retVal.Columns.Add("SiteCode", typeof(Int16));
                retVal.Columns.Add("RetrievalTime", typeof(DateTime));
            }
            else
            {
                retVal.Columns.Add("IEN", typeof(string));
                retVal.Columns.Add("SiteCode", typeof(Int16));
                retVal.Columns.Add("RetrievalTime", typeof(DateTime));
            }

            if (fieldArray != null)
            {
                foreach (string s in fieldArray)
                {
                    retVal.Columns.Add(s, typeof(string)); // add each Vista field using field as column title
                }
            }
            if (wpOrComputedFields != null)
            {
                foreach (string s in wpOrComputedFields)
                {
                    if (String.IsNullOrEmpty(s))
                    {
                        continue;
                    }
                    retVal.Columns.Add(s, typeof(string)); // add each Word Processing or Computed field
                }
            }
            return retVal;
        }


        internal static DataTable generateVerticalDataTable(string vistaFile, bool subfile)
        {
            DataTable retVal = new DataTable();
            retVal.TableName = String.Concat(vistaFile, "_KEYVALUE");
            if (subfile)
            {
                retVal.Columns.Add("P_IEN", typeof(string));
                retVal.Columns.Add("IEN", typeof(string));
                retVal.Columns.Add("SiteCode", typeof(Int16));
                retVal.Columns.Add("RetrievalTime", typeof(DateTime));
            }
            else
            {
                retVal.Columns.Add("IEN", typeof(string));
                retVal.Columns.Add("SiteCode", typeof(Int16));
                retVal.Columns.Add("RetrievalTime", typeof(DateTime));
            }
            retVal.Columns.Add("Field_Key", typeof(string));
            retVal.Columns.Add("Field_Value", typeof(string));
            return retVal;
        }

        internal static DataTable swapColumnNames(DataTable table, Dictionary<string, string> beforeAndAfter)
        {
            IList<String> allKeys = beforeAndAfter.Keys.ToList();
            IList<String> allValues = beforeAndAfter.Values.ToList();

            for (int i = 0; i < allKeys.Count; i++)
            {
                if (table.Columns.Contains(allKeys[i]))
                {
                    table.Columns[allKeys[i]].ColumnName = allValues[i];
                }
            }

            return table;
        }

        internal static String convertDataTableToDelimited(DataTable table, String fieldSeparator, String recordSeparator)
        {
            // might try using this to strip invalid chars (RS & US)
            //bool isSubfileDataTable = false;
            //if (table.Rows.Count > 0 && table.Rows[0][3].GetType() == typeof(DateTime)) // subfiles have extraction timestamp in 4th column (pien, ien, sitecode, ts, field 1, etc..)
            //{
            //    isSubfileDataTable = true;
            //}
            StringBuilder sb = new StringBuilder();

            foreach (DataColumn column in table.Columns)
            {
                sb.Append(column.ColumnName);
                sb.Append(fieldSeparator);
            }
            sb.Remove(sb.Length - 1, 1); // remove last field separator
            sb.Append(recordSeparator); // done with header row

            for (int i = 0; i < table.Rows.Count; i++)
            {
                for (int j = 0; j < table.Columns.Count; j++)
                {
                    sb.Append(table.Rows[i][j]);
                    sb.Append(fieldSeparator);
                }
                sb.Remove(sb.Length - 1, 1); // remove last field separator
                sb.Append(recordSeparator); // done with row
            }

            sb.Remove(sb.Length - 1, 1); // remove last record separator

            return sb.ToString();
        }

        internal static DataTable convertDelimitedToDataTable(String tableName, String delimited, String fieldSeparator, String recordSeparator)
        {
            bool isSubfile = false;

            String[] lines = delimited.Split(new String[] { recordSeparator }, StringSplitOptions.None);

            String headerLine = lines[0];
            String[] columnNames = headerLine.Split(new String[] { fieldSeparator }, StringSplitOptions.None);
            if (String.Equals(columnNames[0], "P_IEN", StringComparison.CurrentCultureIgnoreCase))
            {
                isSubfile = true;
            }

            DataTable table = generateVistaQueryDataTable(tableName, null, isSubfile, null);
            int startColumn = table.Columns.Count;

            for (int i = startColumn; i < columnNames.Length; i++)
            {
                table.Columns.Add(new DataColumn(columnNames[i], typeof(String)));
            }

            for (int i = 1; i < lines.Length; i++)
            {
                String[] currentPieces = lines[i].Split(new String[] { fieldSeparator }, StringSplitOptions.None);
                int j = 3;
                
                if (isSubfile)
                {
                    j++;
                    table.Rows.Add(new object[] { currentPieces[0], currentPieces[1], Convert.ToInt16(currentPieces[2]), Convert.ToDateTime(currentPieces[3]) });
                }
                else
                {
                    table.Rows.Add(new object[] { currentPieces[0], Convert.ToInt16(currentPieces[1]), Convert.ToDateTime(currentPieces[2]) });
                }

                for (; j < columnNames.Length; j++)
                {
                    table.Rows[i-1][j] = currentPieces[j];
                }
            }

            return table;
        }

        public static DataTable addResultsToTable(DataTable destination, DataTable source)
        {
            foreach (DataRow row in source.Rows)
            {
                object[] vals = new object[row.ItemArray.Length];
                row.ItemArray.CopyTo(vals, 0);
                destination.Rows.Add(vals);
            }
            return destination;
        }

        public static DataTable toKeyValTableFromDdrGetsEntry(VistaQuery query, Dictionary<String, String> keyVals, DateTime extractionTimestamp)
        {
            DataTable keyValTable = generateVerticalDataTable(query.VistaFile, query.IsSubFileQuery);

            int currentIndex = query.IsSubFileQuery ? 4 : 3;
            foreach (String key in keyVals.Keys)
            {
                object[] values = new object[5 + (query.IsSubFileQuery ? 1 : 0)]; // 5 columns if not subfile query, add 1 if is subfile query
                if (query.IsSubFileQuery)
                {
                    String ienString = StringUtils.convertIensToIen(query.IENS); // since this comes from DDR GETS ENTRY, the query will contain the IENS
                    values[0] = (ienString).Substring((ienString).IndexOf('_') + 1); // split the string nLevel_(n-1)Level_..._1Level in to two pieces and take the second - this gets rid of current record's IEN piece
                    values[1] = ienString;
                    values[2] = query.SiteCode;
                    values[3] = extractionTimestamp;
                }
                else
                {
                    values[0] = StringUtils.convertIensToIen(query.IENS); // since this comes from DDR GETS ENTRY, the query will contain the IENS - we'll only have one comma piece if not subfile
                    values[1] = query.SiteCode;
                    values[2] = extractionTimestamp;
                }

                values[currentIndex] = key;
                values[currentIndex + 1] = keyVals[key];

                keyValTable.Rows.Add(values);
            }

            return keyValTable;
        }

        /// <summary>
        /// This is a helper function for ticket #16 - the least obtrusive solution seemed to be simply moving the identifier boolean values back out to the the 
        /// end of the DDR strings and placing the WP fields at the end of the regular fields and then using the existing functions to build the DataTable without modification
        /// </summary>
        public static void adjustDdrResultsWithWpAndIdentifiedFiles(VistaQuery query, String[] ddrResults)
        {
            String[] ddrFields = query.Fields.Split(semicolonDelim);
            String[] wpFields = query.WP_Or_Computed_Fields.Split(semicolonDelim);
            String[] identifiedFiles = query.IdentifiedFiles.Split(semicolonDelim);

            // Query: fields = ".01;.03;1", identifiedFiles = "/2.06;/2.98", wp_or_computed = "5;8"
            //                             .01        .03       1       /2.06  /2.98                        5                                                 8
            // sample result coming in: PATIENT,ONE^3010101^123 MAIN ST&#94;0&#94;10[FS]This is some long text I had to get via a WP field call[FS]This is another big long text field
            // which should be transformed to
            //                             .01        .03       1                               5                                                 8              /2.06 /2.98
            //                          PATIENT,ONE^3010101^123 MAIN ST^This is some long text I had to get via a WP field call^This is another big long text field^0^10

            int firstIdentifierIndex = ddrFields.Length + 1; // + 1 to get past fields + IEN 
            int fistWpOrCompIndex = firstIdentifierIndex + identifiedFiles.Length; // then add the number of identified fields
            String[] identifierPlaceholders = new String[identifiedFiles.Length];
            String[] wpPlaceholders = new String[wpFields.Length];
            for (int i = 0; i < ddrResults.Length; i++)
            {
                ddrResults[i] = ddrResults[i].Replace(FIELD_SEPARATOR, "^"); // replace FS with carat
                ddrResults[i] = ddrResults[i].Replace("&#94;", "^"); // replace identifier delim with carat
                String[] allPieces = ddrResults[i].Split(caratDelim);

                // put the identifier flags and WP fields in placeholder arrays
                for (int j = 0; j < identifiedFiles.Length; j++)
                {
                    identifierPlaceholders[j] = allPieces[firstIdentifierIndex + j];
                }

                for (int k = 0; k < wpFields.Length; k++)
                {
                    wpPlaceholders[k] = allPieces[fistWpOrCompIndex + k];
                }

                // finally shift the values around in the string and rebuild the string with carat delimiters
                StringBuilder sb = new StringBuilder();
                for (int n = 0; n < ddrFields.Length + 1; n++) // loop through regular DDR values = don't forget to add + 1 for IEN!
                {
                    sb.Append(allPieces[n]);
                    sb.Append("^");
                }
                for (int k = 0; k < wpFields.Length; k++) // then add the WP fields from the placeholders
                {
                    sb.Append(wpPlaceholders[k]);
                    sb.Append("^");
                }
                for (int j = 0; j < identifiedFiles.Length; j++)
                {
                    sb.Append(identifierPlaceholders[j]);
                    sb.Append("^");
                }
                sb.Remove(sb.Length - 1, 1); // remove trailing carat

                ddrResults[i] = sb.ToString(); // adjusted DDR result ready to roll!
            }

            // done massaging! the ddrResults should be fixed and toQueryResults function below should now work for the adjusted ddrResults!
        }

        public static QueryResults toQueryResultsFromDdr(VistaQuery query, String[] ddrResults)
        {
            QueryResults result = new QueryResults();

            IList<domain.reporting.Exceptional> errors = validateDdrResults(query, ref ddrResults);
            if (errors.Count > 0)
            {
                // no longer throwing an exception if zero results - a few cases found where the end of the file returns some kkoky
                //if (ddrResults.Length == 0) // if all of the records were marked invalid, i guess we should just fail big
                //{
                //    throw new DownstreamException(String.Format("Those DDR results appear to be invalid: {0}", ddrResults[0])) { VistaQuery = query };
                //}
                //else // otherwise just set our exceptions on the query results and we'll handle them further up the stack
                //{
                foreach (domain.reporting.Exceptional e in errors)
                {
                    result.Exceptionals.Add(e);
                }
                //}
            }

            // toQueryResults assumes the ddrResults string contains the WP/Computed fields already (i.e. added elsewhere to each line) - when we added those results, we used the FS character.
            // we need to swap that character back out with a carat so the loop below can parse the results more easily
            String[] wpAndComputed = new String[0];
            if (!String.IsNullOrEmpty(query.WP_Or_Computed_Fields) && String.IsNullOrEmpty(query.Gets_Alignment)) 
            {
                wpAndComputed = query.WP_Or_Computed_Fields.Split(semicolonDelim);
                for (int i = 0; i < ddrResults.Length; i++)
                {
                    ddrResults[i] = ddrResults[i].Replace(FIELD_SEPARATOR, "^");
                }
            }

            String[] fields = query.Fields.Split(semicolonDelim);
            DataTable currentTable = generateVistaQueryDataTable(query.VistaFile, fields, query.IsSubFileQuery, wpAndComputed);
            result.DdrResults.Add(currentTable);

            if (ddrResults == null || ddrResults.Length == 0)
            {
                return result;
            }

            // Prepare to process our results
            Int16 intSiteCode = Convert.ToInt16(query.SiteCode);
            DateTime retrievalTime = DateTime.Now;
            
            bool hasIdentifiedFiles = !String.IsNullOrEmpty(query.IdentifiedFiles);
            String iensString = "";
            String[] identifiedFiles = null;
            int identifiedFilesStartIndex = 0;
            if (hasIdentifiedFiles)
            {
                iensString = query.IENS;
                result.SubQueryIens = new Dictionary<String, IList<String>>();
                identifiedFiles = query.IdentifiedFiles.Split(semicolonDelim);
                identifiedFilesStartIndex = query.Fields.Split(semicolonDelim).Length + 1 + wpAndComputed.Length; // we'll start looking at the resolved file indicators here - per ticket #16, we moved the identifier flags out to the end of the string so we need to go past the WP fields now, too
                foreach (String identifiedFile in identifiedFiles)
                {
                    result.SubQueryIens.Add(identifiedFile, new List<String>()); // only building our container objects here
                }
            }

            String pIenStr = ""; // string for DataTable - looks like third_second_first per db requirements
            if (query.IsSubFileQuery)
            {
                pIenStr = StringUtils.convertIensToIen(query.IENS);
            }

            // Push each valid ddrresult into our datatable
            String enDdiolChar = "&#94;";
            String carat = "^";
            foreach (string ddrResult in ddrResults)
            {
                // Break our string up into it's parts
                string[] ddrValues = ddrResult.Replace(enDdiolChar, carat).Split(caratDelim);

                object[] rowVals = new object[result.DdrResults[0].Columns.Count]; // generateVistaQueryDataTable already determined the correct number of columns

                if (ddrValues.Length < fields.Length + 1) // +1 for the IEN - found some weird random cases where differing data seems to make it's way in to results - let's just ignore it for now
                {
                    logging.Log.LOG("Found a malformed record in this query result! We're ignoring for now but may need to look at it later: " + ddrResult);
                    continue;
                }

                int currentColumnIndex = 3;
                if (query.IsSubFileQuery)
                {
                    rowVals[0] = pIenStr; // SECONDPARENTIEN_FIRSTPARENTIEN <-- converted to format by StringUtils function above
                    rowVals[1] = String.Concat(ddrValues[0], "_", pIenStr); // nthLevel_n-1Level_..._firstLevel
                    rowVals[2] = intSiteCode;
                    rowVals[3] = retrievalTime;
                    currentColumnIndex = 4;
                }
                else
                {
                    rowVals[0] = ddrValues[0];
                    rowVals[1] = intSiteCode;
                    rowVals[2] = retrievalTime;
                }

                for (int i = 1; i < (query.IsSubFileQuery ? currentTable.Columns.Count - 3 : currentTable.Columns.Count - 2); i++) // already stored IEN so start at 1, number of columns minus extra added
                {
                    rowVals[currentColumnIndex] = ddrValues[i];
                    currentColumnIndex++;
                }

                result.DdrResults[0].Rows.Add(rowVals);

                // now take care of identified files
                if (hasIdentifiedFiles)
                {
                    for (int i = 0; i < identifiedFiles.Length; i++)
                    {
                        if (!String.Equals("0", ddrValues[identifiedFilesStartIndex + i]))
                        {
                            if (String.IsNullOrEmpty(iensString)) // if  we don't already have an IENS string started, use a leading comma ',' character
                            {
                                result.SubQueryIens[identifiedFiles[i]].Add("," + ddrValues[0] + ","); // first child subfile - use parent's IEN in column 0
                            }
                            else // already started an IENS string (i.e. in a subfile) - concatenate the current IEN and a comma ',' character
                            {
                                result.SubQueryIens[identifiedFiles[i]].Add("," + ddrValues[0] + iensString); // add the IEN to the correct queue - if we're in a subfile, the IEN is the second column!!!
                            }
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Get subqueries for all the child nodes in a configuration based off the query results
        /// </summary>
        /// <param name="config"></param>
        /// <param name="queryResults"></param>
        /// <returns></returns>
        public static ConcurrentQueue<VistaQuery> getSubQueriesFromResults(ExtractorConfiguration config, QueryResults queryResults)
        {
            ConcurrentQueue<VistaQuery> subQueries = new ConcurrentQueue<VistaQuery>();
            if (queryResults.DdrResults[0].Rows.Count == 0)
            {
                return subQueries;
            }
            String queryResultsSiteCode = Convert.ToString((Int16)queryResults.DdrResults[0].Rows[0]["SiteCode"]);

            foreach (TreeNode<QueryConfiguration> child in config.QueryConfigurations.RootNode.Children)
            {
                foreach (String ien in queryResults.SubQueryIens["/" + child.Value.File]) // TODO - make sure we're checking against correct format (/45/45.06 vs 45.06 
                {
                    VistaQuery current = new VistaQuery(config, child.Value);
                    current.MaxRecords = ""; // set blank because there is no looping in subfiles
                    current.IENS = ien;
                    current.StartIen = current.From = child.Value.From; // ticket #9 bug fix line - didn't uncover in testing because 1) weren't specifiying start IEN 2) not specifying start IEN on ExtractorConfiguration object which is where the property was being copied from. Get it instead directly from TreeNode
                    current.SiteCode = queryResultsSiteCode;
                    current.IsSubFileQuery = true;
                    subQueries.Enqueue(current);
                }
            }
            return subQueries;
        }

        /// <summary>
        /// Build a queue of VistaQuery for a single subfile of an ExtractorConfiguration
        /// </summary>
        /// <param name="config"></param>
        /// <param name="queryResults"></param>
        /// <param name="subfile">This call's subfile will be of the format "/2.98"</param>
        /// <returns></returns>
        public static ConcurrentQueue<VistaQuery> getSubQueriesFromResultsBySubfile(ExtractorConfiguration config, QueryResults queryResults, String subfile)
        {
            ConcurrentQueue<VistaQuery> subQueries = new ConcurrentQueue<VistaQuery>();
            if (queryResults.DdrResults[0].Rows.Count == 0)
            {
                return subQueries;
            }
            String queryResultsSiteCode = Convert.ToString((Int16)queryResults.DdrResults[0].Rows[0]["SiteCode"]);

            TreeNode<QueryConfiguration> queryConfig = config.QueryConfigurations.search(new QueryConfiguration() { File = subfile.Substring(1) }); // search for non fully qualified file name
            if (queryConfig == null || queryConfig.Value == null)
            {
                throw new ArgumentException("The specified subfile ({0}) doesn't exist in the configuration tree!", subfile);
            }

            foreach (String ien in queryResults.SubQueryIens[subfile]) // TODO - make sure we're checking against correct format (/45/45.06 vs 45.06 
            {
                VistaQuery current = new VistaQuery(config, queryConfig.Value);
                current.MaxRecords = ""; // set blank because there is no looping in subfiles
                current.IENS = ien;
                current.StartIen = current.From = queryConfig.Value.From; // ticket #9 bug fix line - didn't uncover in testing because 1) weren't specifiying start IEN 2) not specifying start IEN on ExtractorConfiguration object which is where the property was being copied from. Get it instead directly from TreeNode
                current.SiteCode = queryResultsSiteCode;
                current.IsSubFileQuery = true;
                subQueries.Enqueue(current);
            }
            return subQueries;
        }

        internal static String convertDataTableToDelimited(DataTable table)
        {
            return convertDataTableToDelimited(table, FIELD_SEPARATOR, RECORD_SEPARATOR);
        }

        internal static DataTable convertDelimitedToDataTable(string tableName, string delimited)
        {
            return convertDelimitedToDataTable(tableName, delimited, FIELD_SEPARATOR, RECORD_SEPARATOR);
        }

        /// <summary>
        /// This test loads the file contents as-is, loads the file contents as a DataTable, then compares the hash
        /// of the file contents and the conversion of the DataTable back to a delimited file
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        internal static bool validateDelimitedFileConversion(String filePath)
        {
            FileDao fileDao = new FileDao(false);

            String delimitedFromFile = fileDao.readFile(filePath);
            DataTable tableFromFile = fileDao.loadFromFile(filePath);

            return String.Equals( 
                new MD5Hasher().calculateMD5(delimitedFromFile),
                new MD5Hasher().calculateMD5(convertDataTableToDelimited(tableFromFile)));
        }

        /// <summary>
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        internal static bool validateDelimitedFileFormat(QueryConfiguration config, String filePath)
        {
            FileDao fileDao = new FileDao(false);
            
            DataTable tableFromFile = fileDao.loadFromFile(filePath);

            if (!String.Equals(tableFromFile.TableName, config.File))
            {
                return false;
            }
            if (!StringUtils.isRootNode(config.FullyQualifiedFile) && !tableFromFile.Columns.Contains("P_IEN"))
            {
                return false;
            }
            if (!tableFromFile.Columns.Contains("IEN") || !tableFromFile.Columns.Contains("SiteCode") || !tableFromFile.Columns.Contains("RetrievalTime"))
            {
                return false;
            }

            String[] fields = config.Fields.Split(new char[] { ';' });
            foreach (String field in fields)
            {
                if (!tableFromFile.Columns.Contains(field))
                {
                    return false;
                }
            }

            return true;
        }

        // per #30 - need to validate every single record
        internal static IList<domain.reporting.Exceptional> validateDdrResults(VistaQuery vq, ref String[] ddrResults)
        {
            IList<domain.reporting.Exceptional> exceptions = new List<domain.reporting.Exceptional>();

            if (ddrResults == null || ddrResults.Length <= 0)
            {
                return exceptions; // valid DDR results - just no data!
            }
            IList<String> destination = new List<String>();
            String[] fields = vq.Fields.Split(semicolonDelim);
            // identified files, wp fields
            bool hasIdentifiedFiles = !String.IsNullOrEmpty(vq.IdentifiedFiles);
            bool hasWpOrComputed = (!String.IsNullOrEmpty(vq.WP_Or_Computed_Fields) && String.IsNullOrEmpty(vq.Gets_Alignment)); // WP_OR_COMPUTED should have a value AND GETS_ALIGNMENT should not
            String[] identifiedFiles = new String[0];
            if (hasIdentifiedFiles)
            {
                identifiedFiles = vq.IdentifiedFiles.Split(semicolonDelim);
            }
            String[] wpOrComputed = new String[0];
            if (hasWpOrComputed)
            {
                wpOrComputed = vq.WP_Or_Computed_Fields.Split(semicolonDelim);
            }

            for (int i = 0; i < ddrResults.Length; i++)
            {
                if (hasWpOrComputed && hasIdentifiedFiles) // special case! should have already called adjustDdrResults on this result set so we can check these ones separately
                {
                    if (ddrResults[i].Split(caratDelim).Length != fields.Length + 1 + identifiedFiles.Length + wpOrComputed.Length)
                    {
                        exceptions.Add(new domain.reporting.Exceptional() { Code = domain.reporting.ErrorCode.INVALIDLY_FORMED_RECORD, Message = "Field count + WP field count + identifier field count of adjusted result did not match: " + ddrResults[i] });
                        continue;
                    }
                    else
                    {
                        destination.Add(ddrResults[i]);
                        continue;
                    }
                }

                if (ddrResults[i].Split(caratDelim).Length < (fields.Length + 1 + (hasIdentifiedFiles ? 1 : 0))) // +1 for IEN - if we have identified files, an extra carat is returned with the DDR results
                {
                    exceptions.Add(new domain.reporting.Exceptional() { Code = domain.reporting.ErrorCode.INVALIDLY_FORMED_RECORD, Message = "Field count did not match: " + ddrResults[i] });
                    continue;
                }
                // now check identified files
                if (hasIdentifiedFiles)
                {
                    ddrResults[i] = ddrResults[i].Replace("&#94;", "^");
                    if (ddrResults[i].Split(caratDelim).Length != (fields.Length + identifiedFiles.Length + 1)) // + 1 for IEN
                    {
                        exceptions.Add(new domain.reporting.Exceptional() { Code = domain.reporting.ErrorCode.INVALIDLY_FORMED_RECORD, Message = "Field count + identifier count did not match: " + ddrResults[i] });
                        continue;
                    }
                }
                // now check WP fields
                if (hasWpOrComputed)
                {
                    ddrResults[i] = ddrResults[i].Replace(FIELD_SEPARATOR, "^");
                    if (ddrResults[i].Split(caratDelim).Length != (fields.Length + identifiedFiles.Length + wpOrComputed.Length + 1)) // + 1 for IEN - identified files length will just be zero is none are specified
                    {
                        exceptions.Add(new domain.reporting.Exceptional() { Code = domain.reporting.ErrorCode.INVALIDLY_FORMED_RECORD, Message = "Field count + WP field count did not match: " + ddrResults[i] });
                        continue;
                    }
                }
                // finally... done with all our checks! add to destination which we will set the original String[] to when done with loop
                destination.Add(ddrResults[i]);
            }

            if (destination.Count != ddrResults.Length)
            {
                exceptions.Add(new domain.reporting.Exceptional() { Code = domain.reporting.ErrorCode.INFORMATIONAL, Message = "It appears " + (ddrResults.Length - destination.Count) + " out of " + ddrResults.Length + " records were not valid!" });
                //System.Console.WriteLine("Looks like we skipped a record!");
            }
            ddrResults = destination.ToArray();

            return exceptions;
        }

        internal static DataTable scrubFields(DataTable table, IList<String> columnNames, String replacementText = null)
        {
            String str = "STANDARD REPLACEMENT TEXT";
            if (!String.IsNullOrEmpty(replacementText))
            {
                str = replacementText;
            }

            foreach (DataRow row in table.Rows)
            {
                foreach (String columnName in columnNames)
                {
                    if (row[columnName] != null)
                    {
                        if (!String.IsNullOrEmpty(Convert.ToString(row[columnName])))
                        {
                            row[columnName] = str;
                        }
                    }
                }
            }

            return table;
        }

        internal static string addGetsEntryToDdrResult(string parentDdrResult, string[] getsEntryResult, VistaQuery query)
        {
            // build up string to append: FS[.08]{0}FS[25]{1}FS[1001]{2}
            StringBuilder strWithPlaceHolders = new StringBuilder();
            String[] wpFields = query.WP_Or_Computed_Fields.Split(new char[] { ';' });
            for (int i = 0; i < wpFields.Length; i++)
            {
                strWithPlaceHolders.Append(DataTableUtils.FIELD_SEPARATOR);
                //strWithPlaceHolders.Append("[" + wpFields[i] + "]"); // used only for debugging purposes
                strWithPlaceHolders.Append("{" + i.ToString() + "}");
            }
            String resultToAppend = strWithPlaceHolders.ToString();

            String[] replacementPieces = new String[wpFields.Length];
            int currentIdx = 0;
            for (int i = 0; i < getsEntryResult.Length; i++)
            {
                String line = getsEntryResult[i];
                String[] pieces = line.Split(new char[] { '^' });
                if (pieces.Length < 4) // DDR GETS ENTRY results with data look like: 405.2^2^.08^This is some data
                {
                    continue;
                }

                if (String.Equals("[WORD PROCESSING]", pieces[3], StringComparison.CurrentCultureIgnoreCase))
                {
                    StringBuilder sb = new StringBuilder();
                    while (!String.Equals(getsEntryResult[++i], "$$END$$", StringComparison.CurrentCultureIgnoreCase))
                    {
                        sb.AppendLine(getsEntryResult[i]);
                    }
                    replacementPieces[currentIdx++] = sb.ToString();
                }
                else
                {
                    replacementPieces[currentIdx++] = pieces[3];
                }
            }

            return String.Concat(parentDdrResult, String.Format(resultToAppend, replacementPieces));
        }

        internal static IList<char> getDelimiters()
        {
            return new List<Char>() { Char.Parse(DataTableUtils.RECORD_SEPARATOR), Char.Parse(DataTableUtils.FIELD_SEPARATOR) };
        }
    }
}
