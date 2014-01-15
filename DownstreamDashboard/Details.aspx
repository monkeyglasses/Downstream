<%@ Page Title="Details" Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="Details.aspx.cs" Inherits="DownstreamDashboard2.Details" %>
<asp:Content ID="Content1" ContentPlaceHolderID="HeadContent" runat="server">
</asp:Content>
<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">
    <h2 class='screen-title'>Product Extractions by Site</h2>
        <ul id='details-summary'>
          <li>

            Running
            <span><asp:Label ID="labelRunningCount" runat="server" >180</asp:Label></span>
          </li>
          <li>
            Failures
            <span><asp:Label ID="labelFailureCount" runat="server" >9</asp:Label></span>
          </li>
          <li>
            <select class='filter'>

              <option selected='selected'>Filter sites by</option>
              <option data='all'>All</option>
              <option data='completed'>Completed</option>
              <option data='failed'>Failed</option>
              <option data='inProgress'>In Progress</option>
              <option data='notStarted'>Not Started</option>

            </select>
          </li>
        </ul>

        <ul id='extractions-list'>
            <ul class='extraction-labels'>
                <li>Extraction</li>
                <li>ETL</li>
            </ul>

            <asp:Repeater ID="repeaterStatus" runat="server">
                <ItemTemplate>
                    <li class='extraction-row' data='<%# DataBinder.Eval(Container.DataItem, "statusMarkup") %>'>
                        <ul class='extraction-status'>
                            <li class='site-name'><%# DataBinder.Eval(Container.DataItem, "site") %></li>
                            <%# DataBinder.Eval(Container.DataItem, "progressBarMarkup") %>
                            <li class='status'><%# DataBinder.Eval(Container.DataItem, "status") %></li>
                            <li class='action'>
                                <a href='#'></a>
                            </li>

                            <div style='clear: both;'></div>
                        </ul>

                        <div class='extraction-details'>
                          <ul class='details-list small'>
                            <li>
                              <a class='<%# DataBinder.Eval(Container.DataItem, "statusMarkup") %>' href='#'>
                                <span class='package-name'>ADT</span>
                                <span class='timestamp'><%# DataBinder.Eval(Container.DataItem, "adtFinishedTimestamp") %></span>
                              </a>
                            </li>
                            <li>
                              <a class='<%# DataBinder.Eval(Container.DataItem, "statusMarkup") %>' href='#'>
                                <span class='package-name'>Labs</span>
                                <span class='timestamp'><%# DataBinder.Eval(Container.DataItem, "labFinishedTimestamp") %></span>
                              </a>
                            </li>

                            <li>
                              <a class='<%# DataBinder.Eval(Container.DataItem, "statusMarkup") %>' href='#'>
                                <span class='package-name'>Vitals</span>
                                <span class='timestamp'><%# DataBinder.Eval(Container.DataItem, "vitalsFinishedTimestamp") %></span>
                              </a>
                            </li>
                            <li>
                              <a class='<%# DataBinder.Eval(Container.DataItem, "statusMarkup") %>' href='#'>
                                <span class='package-name'>Pharmacy</span>
                                <span class='timestamp'><%# DataBinder.Eval(Container.DataItem, "pharmaFinishedTimestamp") %></span>
                              </a>
                            </li>
                            <li>
                              <a class='<%# DataBinder.Eval(Container.DataItem, "statusMarkup") %>' href='#'>
                                <span class='package-name'>BCMA</span>
                                <span class='timestamp'><%# DataBinder.Eval(Container.DataItem, "bcmaFinishedTimestamp") %></span>
                              </a>
                            </li>
                            <div class='clear'></div>
                          </ul>
                        </div>

                    </li>
                </ItemTemplate>
            </asp:Repeater>
        </ul>

<%--     
<ul id='extractions-list'>
          <ul class='extraction-labels'>
            <li>Extraction</li>
            <li>ETL</li>
          </ul>

          <li class='extraction-row' data='inProgress'>
            <ul class='extraction-status'>
              <li class='site-name'>Atlanta</li>
              <li class='progress-bar c-i'></li>
              <li class='status'>In-Progress</li>
              <li class='action'>
                <a href='#'></a>
              </li>

              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='completed'>

            <ul class='extraction-status'>
              <li class='site-name'>Seattle</li>
              <li class='progress-bar c-c'></li>
              <li class='status'>Completed</li>
              <li class='action schedule'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>

            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>

                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>

                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='inProgress'>
            <ul class='extraction-status'>
              <li class='site-name'>Palo Alto</li>

              <li class='progress-bar i-n'></li>
              <li class='status'>In-Progress</li>
              <li class='action'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>

              <ul class='details-list small'>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>

                  <a class='in-progress' href='#'>
                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='inProgress'>
            <ul class='extraction-status'>
              <li class='site-name'>Austin</li>
              <li class='progress-bar i-n'></li>

              <li class='status'>In-Progress</li>
              <li class='action'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='completed'>
            <ul class='extraction-status'>
              <li class='site-name'>DC</li>
              <li class='progress-bar c-c'></li>

              <li class='status'>Completed</li>
              <li class='action schedule'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='failed'>
            <ul class='extraction-status'>
              <li class='site-name'>Chicago</li>
              <li class='progress-bar f-n'></li>

              <li class='status'>Failed</li>
              <li class='action logs'>
                <a href='../../img/extractions.zip'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='failed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='completed'>
            <ul class='extraction-status'>
              <li class='site-name'>Dallas</li>
              <li class='progress-bar c-c'></li>

              <li class='status'>Completed</li>
              <li class='action schedule'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='notStarted'>
            <ul class='extraction-status'>
              <li class='site-name'>San Jose</li>
              <li class='progress-bar n-n'></li>

              <li class='status'>Not Started</li>
              <li class='action start'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='failed'>
            <ul class='extraction-status'>
              <li class='site-name'>San Francisco</li>
              <li class='progress-bar f-n'></li>

              <li class='status'>Failed</li>
              <li class='action logs'>
                <a href='../../images/extractions.zip'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='failed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='completed'>
            <ul class='extraction-status'>
              <li class='site-name'>Detroit</li>
              <li class='progress-bar c-c'></li>

              <li class='status'>Completed</li>
              <li class='action schedule'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='notStarted'>
            <ul class='extraction-status'>
              <li class='site-name'>Boston</li>
              <li class='progress-bar n-n'></li>

              <li class='status'>Not Started</li>
              <li class='action start'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='failed'>
            <ul class='extraction-status'>
              <li class='site-name'>Nashville</li>
              <li class='progress-bar c-f'></li>

              <li class='status'>Failed</li>
              <li class='action logs'>
                <a href='../../images/extractions.zip'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='inProgress'>
            <ul class='extraction-status'>
              <li class='site-name'>Portland</li>
              <li class='progress-bar c-i'></li>

              <li class='status'>In-Progress</li>
              <li class='action'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='inProgress'>
            <ul class='extraction-status'>
              <li class='site-name'>Sacramento</li>
              <li class='progress-bar i-n'></li>

              <li class='status'>In-Progress</li>
              <li class='action'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='in-progress' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
          <li class='extraction-row' data='completed'>
            <ul class='extraction-status'>
              <li class='site-name'>Wisconsin</li>
              <li class='progress-bar c-c'></li>

              <li class='status'>Completed</li>
              <li class='action schedule'>
                <a href='#'></a>
              </li>
              <div style='clear: both;'></div>
            </ul>
            <div class='extraction-details'>
              <ul class='details-list small'>

                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>ADT</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>

                    <span class='package-name'>Lab Chem</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Vitals</span>

                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>

                  </a>
                </li>
                <li>
                  <a class='completed' href='#'>
                    <span class='package-name'>Other</span>
                    <span class='timestamp'>1/20 8:20 AM</span>
                  </a>
                </li>

                <div class='clear'></div>
              </ul>
            </div>
          </li>
        </ul>--%>
         
</asp:Content>
