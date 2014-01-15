using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using com.bitscopic.downstream.net.http;
using com.bitscopic.downstream.utils;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.svc;
using System.Threading;
//using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Configuration;

namespace com.bitscopic.downstream.dao.vista
{
    public class WcfVistaDao : IVistaDao
    {
        public Uri BaseUri { get; set; }

        public WcfVistaDao()
        {
            setup(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.QuerySvcURL]);
        }

        public WcfVistaDao(Uri baseUri)
        {
            setup(baseUri);
        }

        void setup(String baseUri)
        {
            setup(new Uri(baseUri));
        }

        void setup(Uri baseUri)
        {
            this.BaseUri = baseUri;
        }

        public String[] ddrLister(String siteId, String vistaFile, String iens, String fields, String flags, String maxRex, 
                                        String from, String part, String xRef, String screen, String identifier)
        {
            if (String.IsNullOrEmpty(flags))
            {
                flags = "IP";
            }
            if (String.IsNullOrEmpty(xRef) || String.Equals("#", xRef))
            {
                xRef = ""; // the '#' character causes the URL to be truncated by IIS - since we control the service, we pass nothing and let the service default to '#'
            }
            HttpClient client = new HttpClient(this.BaseUri);
            String response = client.makeRequest(String.Format("/ddrLister?siteId={0}&vistaFile={1}&iens={2}&fields={3}&flags={4}&maxRex={5}&from={6}&part={7}&xref={8}&screen={9}&identifier={10}",
                siteId, vistaFile, iens, fields, flags, maxRex, from, part, xRef, screen, identifier));
            String[] result = StringUtils.Deserialize<String[]>(response);

            return VistaDaoUtils.stripInvalidChars(result);
        }



        public String[] ddrGetsEntry(String siteId, String vistaFile, String iens, String flds, String flags)
        {
            HttpClient client = new HttpClient(this.BaseUri);
            String response = client.makeRequest(String.Format("/ddrGetsEntry?siteId={0}&vistaFile={1}&iens={2}&fields={3}&flags={4}", siteId, vistaFile, iens, flds, flags));
            //TextArray obj = Deserialize<TextArray>(response);
            //return obj.text;
            return VistaDaoUtils.stripInvalidChars(StringUtils.Deserialize<String[]>(response));
        }

        //public Dictionary<String, String> ddrGetsEntryQuery(VistaQuery query)
        //{
        //    HttpClient client = new HttpClient(this.BaseUri);
        //    String response = client.makeRequest(String.Format("/ddrGetsEntry?siteId={0}&vistaFile={1}&iens={2}&fields={3}&flags={4}", query.SiteCode, query.VistaFile, query.IENS, query.Fields, query.Flags));
        //    return toDictFromDdrGetsEntry(response);
        //}

        //public String ddrGetsEntryQueryWP(VistaQuery query)
        //{
        //    HttpClient client = new HttpClient(this.BaseUri);
        //    String response = client.makeRequest(String.Format("/ddrGetsEntry?siteId={0}&vistaFile={1}&iens={2}&fields={3}&flags={4}", query.SiteCode, query.VistaFile, query.IENS, query.Fields, query.Flags));
        //    return toStringFromDdrGetsEntryWP(response);
        //}

        public String getVariableValueQuery(string site, string arg)
        {
            HttpClient client = new HttpClient(this.BaseUri);
            String response = client.makeRequest(String.Format("/getVariableValue?siteId={0}&arg={1}", site, arg));
            return StringUtils.Deserialize<String>(response);
        }

    }
}
