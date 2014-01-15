<%@ Page Title="Home Page" Language="C#" MasterPageFile="~/Site.master" AutoEventWireup="true"
    CodeBehind="Default.aspx.cs" Inherits="DownstreamDashboard2._Default" %>

<asp:Content ID="HeaderContent" runat="server" ContentPlaceHolderID="HeadContent">
</asp:Content>
<asp:Content ID="BodyContent" runat="server" ContentPlaceHolderID="MainContent">
          <div id='overview-wrapper'>
          <h2 class='screen-title'>Overall Status</h2>
          <div class='left-column pull-left'>
            <div class='data-widget' id='countdown'>
              <h3>Next Run Time</h3>
              <ul class='nav nav-tabs nav-stacked'>
                <li>
                  <a>
                  <asp:Label ID="labelNextRunTime" runat="server">--:--:--</asp:Label>
<%--                    <div class='clock'>
                      <span class='hours'>--:</span>
                      <span class='minutes'>--</span>
                      <span class='seconds'>--</span>
                    </div>
--%>                  </a>
                </li>
              </ul>
              <h3>Last Run Completed</h3>
              <ul class='nav nav-tabs nav-stacked'>
                <li>
                  <a>
                      <asp:Label ID="labelLastRunCompleted" runat="server">--:--:--</asp:Label>
                  </a>
                </li>
              </ul>
            </div>
            <div class='data-widget' id='prioritized'>
<%--              <h3>Prioritized Extractions</h3>
--%>                  <asp:Label ID="labelPrioritizedExtractions" runat="server" />
                    
                <asp:Panel ID="panelPrioritizedExtractions" runat="server" Visible="false">
                    <ul class='nav nav-tabs nav-stacked'>
                        <asp:Repeater ID="repeaterPrioritizedExtractions" runat="server">
                            <ItemTemplate>
                                <li>
                                    <a href='#' id='<%# DataBinder.Eval(Container.DataItem, "SiteName") %>'>
                                    <%# DataBinder.Eval(Container.DataItem, "SiteStatusMarkup") %>
                                    <span class='site-name'><%# DataBinder.Eval(Container.DataItem, "SiteName") %></span>
                                    <span class='site-type'><%# DataBinder.Eval(Container.DataItem, "ExtractionMode") %></span>
                                    <span class='last-update'><%# DataBinder.Eval(Container.DataItem, "LastUpdated") %></span>
                                    </a>
                                </li>
                            </ItemTemplate>
                        </asp:Repeater>
                    </ul>
                </asp:Panel>
              

<%--              <ul class='nav nav-tabs nav-stacked'>
                <li>
                  <a href='atlanta' id='atlanta'>
                    <span class='site-status completed'></span>
                    <span class='site-name'>Atlanta</span>
                    <span class='site-type'>Inc.</span>
                    <span class='last-update'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a href='seattle' id='seattle'>
                    <span class='site-status completed'></span>
                    <span class='site-name'>Seattle</span>
                    <span class='site-type'>Inc.</span>
                    <span class='last-update'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a href='palo_alto' id='palo-alto'>
                    <span class='site-status failed'></span>
                    <span class='site-name'>Palo Alto</span>
                    <span class='site-type'>Inc.</span>
                    <span class='last-update'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a href='austin' id='austin'>
                    <span class='site-status in-progress'></span>
                    <span class='site-name'>Austin</span>
                    <span class='site-type'>Inc.</span>
                    <span class='last-update'>1/20 8:20 AM</span>
                  </a>
                </li>
              </ul>
--%>            </div>
          </div>
          <div class='right-column pull-right'>
            <div id='status-switches'>
              <div class='data-widget status' data='IN-PROGRESS' id='in-progress'>
                <h3>In-Progress</h3>
                <ul class='nav nav-tabs nav-stacked'>
                  <li data='EXTRACTIONS'>
                    <a url='#'>
                      <span class='label'>Extractions</span>
                      <h5>
                        <asp:Label ID="labelActiveExtractionsCount" runat="server">0</asp:Label><span class='small'>/<asp:Label ID="labelQueuedExtractionsCount" runat="server">0</asp:Label></span>
                      </h5>
                    </a>
                  </li>
                  <li data='ETL'>
                    <a url='#'>
                      <span class='label'>ETL</span>
                      <h5><asp:Label ID="labelActiveEtlCount" runat="server">0</asp:Label></h5>
                    </a>
                  </li>
                </ul>
              </div>
              <div class='data-widget status' data='COMPLETED' id='completed'>
                <h3>Completed</h3>
                <ul class='nav nav-tabs nav-stacked'>
                  <li data='EXTRACTIONS'>
                    <a url='#'>
                      <span class='label'>Extractions</span>
                      <h5><asp:Label ID="labelCompletedExtractionsCount" runat="server">0</asp:Label></h5>
                    </a>
                  </li>
                  <li data='ETL'>
                    <a url='#'>
                      <span class='label'>ETL</span>
                      <h5><asp:Label ID="labelCompletedEtlCount" runat="server">0</asp:Label></h5>
                    </a>
                  </li>
                </ul>
              </div>
              <div class='data-widget status' data='FAILED' id='failed'>
                <h3>Failed</h3>
                <ul class='nav nav-tabs nav-stacked'>
                  <li data='EXTRACTIONS'>
                    <a class='active' url='#'>
                      <span class='label'>Extractions</span>
                      <h5><asp:Label ID="labelFailedExtractionsCount" runat="server">0</asp:Label></h5>
                    </a>
                  </li>
                  <li data='ETL'>
                    <a url='#'>
                      <span class='label'>ETL</span>
                      <h5><asp:Label ID="labelFailedEtlCount" runat="server">0</asp:Label></h5>
                    </a>
                  </li>
                </ul>
              </div>
            </div>
            <div id='details-grid'>

                <div id='details-grid-failed-extractions' class='active'>
                  <h3>Failed - Extractions</h3>
                  <asp:DataGrid CssClass='table' ID="datagridFailedExtractions" AutoGenerateColumns="false" runat="server">
                    <AlternatingItemStyle CssClass='odd' />

                    
                    <Columns>
                        <asp:BoundColumn DataField="SiteId" Visible="false" />
                        <asp:BoundColumn DataField="SiteName" HeaderText="SITE" HeaderStyle-CssClass="details-grid-table-head" />
                        <asp:BoundColumn DataField="ExtractionMode" HeaderText="TYPE" HeaderStyle-CssClass="details-grid-table-head" />
                        <%-- <asp:BoundColumn DataField="Timestamp" /> --%>
                        <asp:TemplateColumn ItemStyle-CssClass="view-log">
                            <ItemTemplate><a class="view-log" href='Default.aspx?getLog&site=<%# DataBinder.Eval(Container.DataItem, "SiteId") %>&vistaFile=<%# DataBinder.Eval(Container.DataItem, "VistaFile") %>&batchId=<%# DataBinder.Eval(Container.DataItem, "BatchId") %>' /></ItemTemplate>
                        </asp:TemplateColumn>
                    </Columns>
                  </asp:DataGrid>
              </div>

                <div id='details-grid-in-progress-extractions' class='hidden'>
                  <h3>In-Progress - Extractions</h3>
                  <asp:DataGrid CssClass='table' ID="datagridInProgressExtractions" AutoGenerateColumns="false" runat="server">
                    <AlternatingItemStyle CssClass='odd' />

                    <Columns>
                        <asp:BoundColumn DataField="SiteId" Visible="false" />
                        <asp:BoundColumn DataField="SiteName" />
                        <asp:BoundColumn DataField="ExtractionMode" />
                        <%-- <asp:BoundColumn DataField="Timestamp" /> --%>
                        <asp:TemplateColumn ItemStyle-CssClass="view-status">
<%--                            <ItemTemplate><a class="view-log" href='Default.aspx?getStatus&hostname=<%# DataBinder.Eval(Container.DataItem, "Hostname") %>&port=<%# DataBinder.Eval(Container.DataItem, "Port") %>' /></ItemTemplate>
--%> 
                            <ItemTemplate><a class="view-status" href='#' /></ItemTemplate>
                       </asp:TemplateColumn>
                    </Columns>
                  </asp:DataGrid>
              </div>

              <div id='details-grid-completed-extractions' class='hidden'>
                  <h3>Completed - Extractions</h3>
                  <asp:DataGrid CssClass='table' ID="datagridCompletedExtractions" AutoGenerateColumns="false" runat="server">
                    <AlternatingItemStyle CssClass='odd' />

                    <Columns>
                        <asp:BoundColumn DataField="SiteId" Visible="false" />
                        <asp:BoundColumn DataField="SiteName" />
                        <asp:BoundColumn DataField="ExtractionMode" />
                        <%-- <asp:BoundColumn DataField="Timestamp" /> --%>
                        <asp:TemplateColumn ItemStyle-CssClass="view-log">
                            <ItemTemplate><a class="view-log" href='Default.aspx?getLog&site=<%# DataBinder.Eval(Container.DataItem, "SiteId") %>&vistaFile=<%# DataBinder.Eval(Container.DataItem, "VistaFile") %>&batchId=<%# DataBinder.Eval(Container.DataItem, "BatchId") %>' /></ItemTemplate>
                        </asp:TemplateColumn>
                    </Columns>
                  </asp:DataGrid>
              </div>

              <%--<table class='table'>
                <thead>
                  <tr>
                    <th>Site</th>
                    <th>Type</th>
                    <th>Last Run</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  <tr class='odd'>
                    <td>Atlanta</td>
                    <td>Inc.</td>
                    <td>1/20 8:20 AM</td>
                    <td class='view-log'>
                      <a href='../../images/extractions.zip'></a>
                    </td>
                  </tr>
                  <tr class='even'>
                    <td>Seattle</td>
                    <td>Inc.</td>
                    <td>1/20 8:20 AM</td>
                    <td class='view-log'>
                      <a href='../../images/extractions.zip'></a>
                    </td>
                  </tr>
                  <tr class='odd'>
                    <td>Palo Alto</td>
                    <td>Inc.</td>
                    <td>1/20 8:20 AM</td>
                    <td class='view-log'>
                      <a href='../../images/extractions.zip'></a>
                    </td>
                  </tr>
                  <tr class='even'>
                    <td>Austin</td>
                    <td>Inc.</td>
                    <td>1/20 8:20 AM</td>
                    <td class='view-log'>
                      <a href='../../images/extractions.zip'></a>
                    </td>
                  </tr>
                  <tr class='odd'>
                    <td>DC</td>
                    <td>Inc.</td>
                    <td>1/20 8:20 AM</td>
                    <td class='view-log'>
                      <a href='../../images/extractions.zip'></a>
                    </td>
                  </tr>
                </tbody>
              </table>--%>
            </div>
          </div>
          <div class='clear' style='clear: both;'></div>
        </div>

</asp:Content>
