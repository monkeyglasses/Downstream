using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using com.bitscopic.downstream.net;
using com.bitscopic.downstream.domain;
using System.Configuration;

namespace DownstreamDashboard
{
    public partial class Status : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            try
            {
                string arg = Request.QueryString["hostname"];
                string hostname = arg.Substring(0, arg.IndexOf(':'));
                string port = arg.Substring(hostname.Length + 1);

                Client dashboardClient = new Client();
                dashboardClient.connect(hostname, Convert.ToInt32(port));
                String message = dashboardClient.sendJobStatusRequest();
                labelMessage.Text = message.Replace(Environment.NewLine, "<br />");

                decimal trash = 0;
                if (Decimal.TryParse(message, out trash))
                {
                    literalPercentageComplete.Text = Math.Floor(trash * 100).ToString();
                }
                else
                {
                    labelMessage.Text = message.Replace(Environment.NewLine, "<br />");
                }
                //string percentComplete = dashboardClient.sendJobStatusRequest();
                //literalPercentageComplete.Text = percentComplete;
                //labelMessage.Text = "The extractor is " + percentComplete + "% complete!";
            }
            catch (System.Net.Sockets.SocketException se)
            {
                panelGraph.Visible = false;
                labelMessage.Text = "Unable to connect to extractor: " + se.Message;
            }
            catch (Exception)
            {
                panelGraph.Visible = false;
                labelMessage.Text = "The extractor appears to be no longer running!";
            }
        }
    }
}