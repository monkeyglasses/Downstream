using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using com.bitscopic.downstream.dao.file;
using com.bitscopic.downstream.dao.sql;
using com.bitscopic.downstream.domain;
using System.Configuration;
using System.IO;
using System.Threading.Tasks;
using gov.va.medora.mdo.dao.ldap;
using System.ComponentModel;
using System.Threading;

namespace com.bitscopic.downstream
{
    public partial class EtlGui : System.Web.UI.Page
    {
        IList<EtlDownstreamMapItem> _mapItems;
        String _informaticaArchivePathWithDriveLetter;
        String _informaticaArchivePathUnix;
        String _sqlProvider;
        String _sqlConnectionString;
        String _triggerDirectory;

        protected void Page_Load(object sender, EventArgs e)
        {
            if (Session != null && Session["MapItems"] != null)
            {
                _mapItems = Session["MapItems"] as IList<EtlDownstreamMapItem>;
            }

            if (String.Equals(dropDownEnvironmentSelector.SelectedValue, "dev"))
            {
                labelMessage.Text = "Dev ETL/Downstream Map";
                _informaticaArchivePathWithDriveLetter = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.InformaticaArchivePathWithDriveLetter];
                _informaticaArchivePathUnix = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.InformaticaArchivePathUnix];
                _sqlProvider = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.SqlProvider];
                _sqlConnectionString = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.SqlConnectionString];
                _triggerDirectory = ConfigurationManager.AppSettings["devDownstreamTriggerFilePath"];
            }
            else if (String.Equals(dropDownEnvironmentSelector.SelectedValue, "pp"))
            {
                labelMessage.Text = "Pre-prod ETL/Downstream Map";
                _informaticaArchivePathWithDriveLetter = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.InformaticaArchivePathWithDriveLetter];
                _informaticaArchivePathUnix = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.InformaticaArchivePathUnix];
                _sqlProvider = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.SqlProvider];
                _sqlConnectionString = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.SqlConnectionString];
                _triggerDirectory = ConfigurationManager.AppSettings["ppDownstreamTriggerFilePath"];
            }

            if (!Page.IsPostBack)
            {
                bindDataGrid();
            }

            _mapItems = Session["MapItems"] as IList<EtlDownstreamMapItem>;
        }

        protected void dropDownEnvironmentClick(Object sender, EventArgs e)
        {
            if (String.Equals(dropDownEnvironmentSelector.SelectedValue, "dev"))
            {
                labelMessage.Text = "Dev ETL/Downstream Map";
                _informaticaArchivePathWithDriveLetter = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.InformaticaArchivePathWithDriveLetter];
                _informaticaArchivePathUnix = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.InformaticaArchivePathUnix];
                _sqlProvider = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.SqlProvider];
                _sqlConnectionString = ConfigurationManager.AppSettings["dev" + config.AppConfigSettingsConstants.SqlConnectionString];
                _triggerDirectory = ConfigurationManager.AppSettings["devDownstreamTriggerFilePath"];
            }
            else if (String.Equals(dropDownEnvironmentSelector.SelectedValue, "pp"))
            {
                labelMessage.Text = "Pre-prod ETL/Downstream Map";
                _informaticaArchivePathWithDriveLetter = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.InformaticaArchivePathWithDriveLetter];
                _informaticaArchivePathUnix = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.InformaticaArchivePathUnix];
                _sqlProvider = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.SqlProvider];
                _sqlConnectionString = ConfigurationManager.AppSettings["pp" + config.AppConfigSettingsConstants.SqlConnectionString];
                _triggerDirectory = ConfigurationManager.AppSettings["ppDownstreamTriggerFilePath"];
            }

            bindDataGrid();
        }

        void bindDataGrid()
        {
            try
            {
                ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(_sqlProvider, _sqlConnectionString));
                //ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider], 
                //ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString]));
                IList<EtlDownstreamMapItem> items = sqlDao.getEtlDownstreamMap();

                if (checkboxShowLastBatchOnly.Checked) // filter by site and highest batch ID number
                {
                    Dictionary<String, EtlDownstreamMapItem> filteredCollection = new Dictionary<string, EtlDownstreamMapItem>();
                    foreach (EtlDownstreamMapItem mapItem in items)
                    {
                        if (!filteredCollection.ContainsKey(mapItem.SiteId))
                        {
                            filteredCollection.Add(mapItem.SiteId, mapItem);
                            continue;
                        }

                        Int32 currentEtlBatchAsInt = Convert.ToInt32(mapItem.EtlBatchId);
                        Int32 filteredCollectionEtlBatchAsInt = Convert.ToInt32(filteredCollection[mapItem.SiteId].EtlBatchId);

                        if (currentEtlBatchAsInt > filteredCollectionEtlBatchAsInt)
                        {
                            filteredCollection[mapItem.SiteId] = mapItem; // overwrite with current map item
                        }
                    }

                    EtlDownstreamMapItem[] temp = new EtlDownstreamMapItem[filteredCollection.Count];
                    filteredCollection.Values.CopyTo(temp, 0);
                    items = temp.ToList();
                }

                dataGridEtlDownstreamMap.DataSource = items;
                dataGridEtlDownstreamMap.DataBind();

                Session["MapItems"] = items;
            }
            catch (Exception exc)
            {
                labelMessage.Text = "There was a problem fetching the map table from the database: " + exc.Message;
            }
        }

        protected void rerunClick(object sender, DataGridCommandEventArgs e)
        {
            try
            {
                int index = e.Item.ItemIndex;
                EtlDownstreamMapItem clickedItem = _mapItems[index];

                ControlCollection checkboxControls = e.Item.Cells[3].Controls;

                CheckBox cbPatAdt = (CheckBox)e.Item.FindControl("checkboxPatAdt");
                CheckBox cbLabMicro = (CheckBox)e.Item.FindControl("checkboxLabMicro");
                CheckBox cbPharm = (CheckBox)e.Item.FindControl("checkboxPharm");
                CheckBox cbBcma = (CheckBox)e.Item.FindControl("checkboxBcma");
                CheckBox cbVitals = (CheckBox)e.Item.FindControl("checkboxVitals");

                String[] flagArray = new String[5];
                flagArray[0] = (cbPatAdt.Checked ? "1" : "0");
                flagArray[1] = (cbLabMicro.Checked ? "1" : "0");
                flagArray[2] = (cbPharm.Checked ? "1" : "0");
                flagArray[3] = (cbBcma.Checked ? "1" : "0");
                flagArray[4] = (cbVitals.Checked ? "1" : "0");

                String flagString = (cbPatAdt.Checked ? "1" : "0") + "," + (cbLabMicro.Checked ? "1" : "0") + "," + (cbPharm.Checked ? "1" : "0") + "," + (cbBcma.Checked ? "1" : "0") + "," + (cbVitals.Checked ? "1" : "0");

                if (!flagArray.Contains("1"))
                {
                    labelMessage.Text = "You should turn on at least one flag dummy...";
                    return;
                }
                if (File.Exists("C:\\inetpub\\wwwroot\\dashboard\\temp\\config" + clickedItem.EtlBatchId + "_" + clickedItem.DownstreamBatchId + ".txt"))
                {
                    labelMessage.Text = "It appears you have already attempted to begin re-processing this batch - please wait for the current job to finish";
                    return;
                }

                logging.Log.LOG("Starting re-run... Flags: " + String.Join(",", flagArray));

                String configFileContents = clickedItem.EtlBatchId + "\n" + clickedItem.DownstreamBatchId + "\n" + clickedItem.SiteId + "\n" + _informaticaArchivePathUnix +
                        "\n" + _triggerDirectory + "\n" + _informaticaArchivePathWithDriveLetter + "\n" + flagString + "\n" +
                        ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.NetworkUserDomain] + "\n" +
                        ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.NetworkUserName] + "\n" +
                        ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.NetworkUserPassword];
                new FileDao(false).saveToFile(configFileContents, "C:\\inetpub\\wwwroot\\dashboard\\temp\\config" + clickedItem.EtlBatchId + "_" + clickedItem.DownstreamBatchId + ".txt"); // will get picked up by utility

                labelMessage.Text = "Launched the reprocessing utility! You should check the logs later to verify the job completed successfully";
                return;
            }
            catch (Exception exc)
            {
                labelMessage.Text = "Oops... Looks like something went wrong trying to find the item you clicked - try reloading the web page <p>" + exc.Message + "</p>";
            }
        }

        protected void ViewLogClick(Object sender, EventArgs e)
        {
            if (textBoxLog.Visible)
            {
                textBoxLog.Visible = false;
                buttonViewLog.Text = "Show Log";
                textBoxLog.Text = "";
            }
            else
            {
                textBoxLog.Visible = true;
                buttonViewLog.Text = "Hide Log";
                textBoxLog.TextMode = TextBoxMode.MultiLine;

                try
                {
                    textBoxLog.Text = new FileDao(false).readFile(ConfigurationManager.AppSettings["ReproccessLogPath"]);
                }
                catch (Exception exc)
                {
                    labelMessage.Text = "Sad face. Couldn't open the log... " + exc.Message;
                }
            }
        }

    }
}