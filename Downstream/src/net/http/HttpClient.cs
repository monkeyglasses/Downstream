using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using com.bitscopic.downstream.utils;

namespace com.bitscopic.downstream.net.http
{
    public class HttpClient
    {
        Uri _baseUri;

        public HttpClient(Uri baseUri)
        {
            _baseUri = baseUri;
        }

        public string makeRequest(string uri)
        {
            uri = uri.Replace(" ", "%20");
            WebRequest request = WebRequest.Create(String.Concat(_baseUri, uri));
            request.Method = "GET";
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            Stream stream = response.GetResponseStream();
            StreamReader rdr = new StreamReader(stream);

            if (response.StatusCode == HttpStatusCode.OK)
            {
                String responseBody = rdr.ReadToEnd();
                return StringUtils.stripChars(responseBody, DataTableUtils.getDelimiters());
                //byte[] buffer = new byte[response.ContentLength];
                //response.GetResponseStream().Read(buffer, 0, (Int32)response.ContentLength);
                //return System.Text.Encoding.UTF8.GetString(buffer);
            }
            else
            {
                return response.StatusCode.ToString();
                // TODO - handle error
            }
        }
    }
}
