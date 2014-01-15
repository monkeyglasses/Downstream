using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.SessionState;

namespace DownstreamDashboard
{
    public class Global : System.Web.HttpApplication
    {

        void Application_Start(object sender, EventArgs e)
        {
            // Code that runs on application startup

        }

        void Application_End(object sender, EventArgs e)
        {
            //  Code that runs on application shutdown

        }

        void Application_Error(object sender, EventArgs e)
        {
            // Code that runs when an unhandled error occurs

        }

        void Session_Start(object sender, EventArgs e)
        {
            // Code that runs when a new session is started

        }

        void Session_End(object sender, EventArgs e)
        {
            // Code that runs when a session ends. 
            // Note: The Session_End event is raised only when the sessionstate mode
            // is set to InProc in the Web.config file. If session mode is set to StateServer 
            // or SQLServer, the event is not raised.

        }

        void Application_BeginRequest(object sender, EventArgs e)
        {
            object trash = Response.Filter;
            Response.Filter = new com.bitscopic.downstream.domain.ResponseReader(Response.OutputStream);

            if (!String.IsNullOrEmpty(Request.QueryString["callback"]))
            {
                ((com.bitscopic.downstream.domain.ResponseReader)Response.Filter).JsonpCallback = Request.Params["callback"];
            }
        }

        void Application_EndRequest(object sender, EventArgs e)
        {
            com.bitscopic.downstream.domain.ResponseReader response = (com.bitscopic.downstream.domain.ResponseReader)Response.Filter;

            if (response.JsonpResponseLength > 0) // if added JSONP callback, need to set correct content length headers
            {
                Response.ClearHeaders();
                Response.AppendHeader("Content-Length", response.JsonpResponseLength.ToString());
                Response.AppendHeader("Content-Type", "application/json; charset=utf-8");
            }
        }
    }
}
