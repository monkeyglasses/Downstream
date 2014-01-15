using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.dao.vista;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using System.Configuration;

namespace com.bitscopic.downstream.utils
{
    public class LabChemUtils
    {
        public IList<String> IensWithComment = new List<String>();

        Char FIELD_DELIM = '\x1f';
        Char RECORD_DELIM = '\x1e';
        // commenting out because not using data dictionary for test names any more
        //Dictionary<String, String> _fieldNamesDict;

        //public LabChemUtils(Dictionary<String, String> site63x04DataDictionary)
        //{
        //    _fieldNamesDict = site63x04DataDictionary;
        //}

        public static bool containsLabChemConfig(ExtractorConfiguration config)
        {
            if (String.Equals(config.QueryConfigurations.RootNode.Value.File, "63") && config.QueryConfigurations.search(new QueryConfiguration() { File = "63.04" }) != null)
            {
                return true;
            }
            return false;
        }

        public Dictionary<String, IList<String>> parseLabChemDdrResultsAndSetCommentFlags(VistaQuery query, String[] results)
        {
            Dictionary<String, IList<String>> testsByIens = new Dictionary<String, IList<String>>();

            String parentIen = query.IENS.Replace(",", "");
            //StringBuilder sb = new StringBuilder();
            for (int i = 0; i < results.Length; i++) // String s in subResults)
            {
                String currentIen = results[i].Split(new char[] { '^' })[0];

                // comment was written out first so let's get that
                String[] ddrPartAndIds = results[i].Split(new String[] { "&#94;" }, StringSplitOptions.None);
                int firstTilde = ddrPartAndIds[1].IndexOf('~');

                String commentFlag = "";
                String restOfIdentifier = "";
                if (firstTilde > 0)
                {
                    commentFlag = ddrPartAndIds[1].Substring(0, firstTilde);
                    restOfIdentifier = ddrPartAndIds[1].Substring(firstTilde + 1);
                }
                else // no tests!!! only have comments flag
                {
                    commentFlag = ddrPartAndIds[1].Trim();
                    restOfIdentifier = "";
                }

                if (!String.Equals(commentFlag, "0"))
                {
                    this.IensWithComment.Add(String.Concat(",", currentIen, query.IENS));
                }

                // now that we've obtained the comment, let's remove the flag for comment for getAllValsFromDelimited doesn't need to be modified
                results[i] = String.Concat(ddrPartAndIds[0], "&#94;", restOfIdentifier);

                String[] multipleLines = getAllValsFromDelimited(results[i]);

                IList<String> tests = new List<String>();
                for (int j = 0; j < multipleLines.Length; j++)
                {
                    String recordWithoutIen = multipleLines[j].Substring(multipleLines[j].IndexOf('^') + 1); // remove the IEN from the DDR results string
                    multipleLines[j] = parentIen + FIELD_DELIM + String.Concat(parentIen, "_", currentIen) + FIELD_DELIM + query.SiteCode + FIELD_DELIM + System.DateTime.Now.ToString() + FIELD_DELIM + recordWithoutIen.Replace('^', FIELD_DELIM);
                    multipleLines[j] = String.Concat(multipleLines[j], FIELD_DELIM); // add one more field delimiter in prep for comment field
                    //sb.Append(multipleLines[j]);
                    //sb.Append(RECORD_DELIM);
                    tests.Add(multipleLines[j]);
                }

                testsByIens.Add(String.Concat(",", currentIen, query.IENS), tests); // e.g. ,6899697.918861,277, -> UREA NITROGEN, WBC, RBC, test 4, etc...
            }

            return testsByIens;
        }

        public QueryResults parseLabChemDdrResults(VistaQuery query, String[] results)
        {
            Dictionary<String, IList<String>> testsByIens = parseLabChemDdrResultsAndSetCommentFlags(query, results);
            Dictionary<String, String> commentsByIens = VistaDaoUtils.getCommentsForIens(query.SiteCode, this.IensWithComment);
            mergeLabChemCommentsWithParsedResults(commentsByIens, testsByIens);

            return new QueryResults() { StringResult = buildResults(testsByIens) };
        }

        internal String buildResults(Dictionary<String, IList<String>> testsByIens)
        {
            StringBuilder sb = new StringBuilder();

            foreach (String key in testsByIens.Keys)
            {
                //bool testHasComments = commentsByIens.ContainsKey(key);

                foreach (String testStr in testsByIens[key])
                {
                    sb.Append(testStr);

                    //if (testHasComments)
                    //{
                    //    sb.Append(commentsByIens[key]);
                    //}

                    sb.Append(RECORD_DELIM);
                }
            }

            return sb.ToString();
        }

        internal String[] getAllValsFromDelimited(String delimitedTest)
        {
            String[] pieces = delimitedTest.Split(new String[] { "&#94;" }, StringSplitOptions.None);
            String commonPiece = pieces[0];
            String testPiece = pieces[1];

            if (String.IsNullOrEmpty(testPiece))
            {
                return new String[1] { String.Concat(pieces[0], "^^^^^^") }; // make sure to update the number of carats if the number of fields changes!!!
            }

            String[] individualTests = testPiece.Split(new String[] { "~" }, StringSplitOptions.None);

            String[] newLines = new String[individualTests.Length];

            // wrap .111 in quotes - found comma in this field
            String[] commonPieces = commonPiece.Split(new char[] { '^' });
            // String newX11 = String.Concat("\"", commonPieces[6], "\"");
            // String newX111 = String.Concat("\"", commonPieces[7], "\"");
            commonPiece = String.Concat(commonPieces[0], "^", commonPieces[1], "^", commonPieces[2], "^", commonPieces[3], "^", commonPieces[4], "^", commonPieces[5], "^", commonPieces[6], "^", commonPieces[7]);

            for (int i = 0; i < individualTests.Length; i++)
            {
                Int32 firstColonIdx = individualTests[i].IndexOf(':');
                
                //Int32 secondColonIdx = individualTests[i].IndexOf(':', firstColonIdx + 1);
                //String crossRefIen = "";
                //if (secondColonIdx > 0)
                //{
                //    crossRefIen = individualTests[i].Substring(firstColonIdx + 1, secondColonIdx - firstColonIdx - 1);
                //    firstColonIdx = secondColonIdx;
                //    if (String.IsNullOrEmpty(crossRefIen))
                //    {
                //        System.Console.WriteLine("Cross reference file 60 lookup failed!");
                //        logging.Log.LOG("Lab cross ref IEN and value in lab are different: " + commonPiece);
                //    }
                //}

                // commented back in because M code says join data dictionary field number on file 60, field 13
                String fieldNo = individualTests[i].Substring(0, firstColonIdx);                 // commenting out because not using data dictionary for test names any more
                
                String restOfIndividualTest = individualTests[i].Substring(firstColonIdx + 1);
                String[] currentPieces = gov.va.medora.utils.StringUtils.split(restOfIndividualTest, gov.va.medora.utils.StringUtils.CARET);
                //String[] fieldAndGlobal = StringUtils.split(individualTests[i], StringUtils.COLON);
                //String fieldNo = fieldAndGlobal[0];
                //String[] currentPieces = StringUtils.split(fieldAndGlobal[1], StringUtils.CARET);

                String testValue = currentPieces[0];
                String interpretation = "";
                String wkldCode = "";
                String refHigh = "";
                String refLow = "";
                String units = "";
                String testIen = "";

                if (currentPieces.Length > 1)
                {
                    interpretation = currentPieces[1];
                }
                if (currentPieces.Length > 2)
                {
                    wkldCode = currentPieces[2];
                    if (wkldCode.Contains("!"))
                    {
                        wkldCode = wkldCode.Substring(0, wkldCode.IndexOf("!"));
                    }
                    String[] ienPieces = gov.va.medora.utils.StringUtils.split(currentPieces[2], "!");
                    if (ienPieces.Length > 6)
                    {
                        testIen = ienPieces[6];
                    }
                }
                if (currentPieces.Length > 4)
                {
                    String[] detailPieces = gov.va.medora.utils.StringUtils.split(currentPieces[4], "!");
                    if (detailPieces != null && detailPieces.Length > 0)
                    {
                        if (detailPieces.Length > 2)
                        {
                            refLow = detailPieces[1];
                            refHigh = detailPieces[2];
                        }

                        if (detailPieces.Length > 6)
                        {
                            units = detailPieces[6];
                        }
                    }
                }

                //if (!String.Equals(testIen, crossRefIen)
                //    && !String.IsNullOrEmpty(crossRefIen))
                //{
                //    testIen = crossRefIen; // correct with lookup value
                //    //System.Console.WriteLine("Lab cross ref IEN and value in lab are different: " + commonPiece);
                //    logging.Log.LOG("Lab cross ref IEN and value in lab are different: " + commonPiece);
                //}

                // commenting out because not using data dictionary for test names any more
                //if (!String.IsNullOrEmpty(fieldNo) && _fieldNamesDict.ContainsKey(fieldNo))
                //{
                //    fieldNo = _fieldNamesDict[fieldNo];
                //    // fieldNo = String.Concat(fieldNamesDict[fieldNo], ("(field #: " + fieldNo)); // we don't care about the field number - really just for debugging and exploration
                //}

                // commenting out because not using data dictionary for test names any more
                // commented back in because M code says join data dictionary field number on file 60, field 13
                newLines[i] = commonPiece + "^" + fieldNo + "^" + testValue + "^" + interpretation + "^" + refLow + "^" + refHigh + "^" + units; //^" + orderedTest;
               // newLines[i] = commonPiece + "^" + testIen + "^" + testValue + "^" + interpretation + "^" + refLow + "^" + refHigh + "^" + units; //^" + orderedTest;
            }

            return newLines;
        }

        /// <summary>
        /// The values for file 63.04 are static - you should set the FROM parameter after adding this config
        /// </summary>
        /// <param name="labFileTree"></param>
        internal static void addStaticLabChemConfig(Tree<QueryConfiguration> labFileTree)
        {
            if (!String.Equals(labFileTree.RootNode.Value.File, "63"))
            {
                throw new ArgumentException("This static lab chem config should only be added to the 63 file tree...");
            }
            labFileTree.RootNode.Value.IdentifiedFiles = "/63.04;/63.05";
            labFileTree.RootNode.addChild(new TreeNode<QueryConfiguration>(new QueryConfiguration()
            {
                Fields = ".01;.03;.05;.06;.1;.11;.112",
                File = "63.04",
                FullyQualifiedFile = "/63/63.04",
                Identifier = "N GBL,IEN,SIEN,STIDX,IDX,VALS,X S X=$D(^(1)) D EN^DDIOL(X) S GBL=$NA(^(0)) I GBL'=\"\" S IEN=$QS(GBL,1) S SIEN=$QS(GBL,3) S STIDX=$O(^LR(IEN,\"CH\",SIEN,1)) I +STIDX'<1 S CNT=0 F IDX=STIDX:0 S CNT=CNT+1 S VALS=$G(^LR(IEN,\"CH\",SIEN,IDX)) D EN^DDIOL(IDX_\":\"_VALS) S IDX=$O(^LR(IEN,\"CH\",SIEN,IDX)) I ((+IDX<1)!(CNT>1000)) Q",
                Packed = false,
                XREF = "B"
            }));
        }


        internal void mergeLabChemCommentsWithParsedResults(Dictionary<String, String> commentsDict, Dictionary<String, IList<String>> testsByIens)
        {
            for (int i = 0; i < commentsDict.Count; i++)
            {
                String currentIens = commentsDict.Keys.ElementAt(i);
                String currentComment = commentsDict[currentIens];

                IList<String> mergedTests = testsByIens[currentIens];
                for (int j = 0; j < mergedTests.Count; j++)
                {
                    mergedTests[j] = String.Concat(mergedTests[j], currentComment);
                }
                testsByIens[currentIens] = mergedTests;
            }
        }

    }
}
