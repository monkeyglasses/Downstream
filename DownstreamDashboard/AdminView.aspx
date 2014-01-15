<%@ Page Title="Home Page" Language="C#" MasterPageFile="~/OldSite.master" AutoEventWireup="true"
    CodeBehind="AdminView.aspx.cs" Inherits="DownstreamDashboard.AdminView" %>

<asp:Content ID="HeaderContent" runat="server" ContentPlaceHolderID="HeadContent">
</asp:Content>
<asp:Content ID="BodyContent" runat="server" ContentPlaceHolderID="MainContent">

    <asp:Label ID="labelMessage" runat="server" ForeColor="Red" Font-Size="Larger" Font-Bold="true" />

    <br />
    <br />
    <h3 class="tableTitles">Active Extractions (<asp:Label ID="labelActiveCount" runat="server" />):</h3>
    <asp:DataGrid ID="datagridActive" AutoGenerateColumns="false" runat="server">
        <Columns>
            <asp:BoundColumn DataField="SITECODE" HeaderText="Site" /> 

            <asp:BoundColumn DataField="VISTAFILE" HeaderText="Vista&nbsp;File" />
            
            <asp:TemplateColumn HeaderText="Extractor Information" >
                <ItemTemplate>
                    <a href='Status.aspx?hostname=<%# DataBinder.Eval(Container.DataItem, "HOSTNAME")%>' target="_blank">
                        <%# DataBinder.Eval(Container.DataItem, "HOSTNAME") %>
                    </a>
                </ItemTemplate>
            </asp:TemplateColumn>  

            <asp:BoundColumn DataField="TIMESTAMP" HeaderText="Started&nbsp;On" />
        </Columns>
    </asp:DataGrid>

    <h3 class="tableTitles">Queued Extractions (<asp:Label ID="labelQueuedCount" runat="server" />):</h3>
    <asp:DataGrid ID="datagridQueued" AutoGenerateColumns="false" runat="server">
        <Columns>
            <asp:BoundColumn DataField="Key" HeaderText="Vista&nbsp;File" />
            
            <asp:BoundColumn DataField="Value" HeaderText="Sites" /> 
        </Columns>
    </asp:DataGrid>

    <h3 class="tableTitles">Completed Extractions (<asp:Label ID="labelCompletedCount" runat="server" />):</h3>
    <asp:DataGrid ID="datagridCompleted" AutoGenerateColumns="false" runat="server">
        <Columns>
            <asp:BoundColumn DataField="Key" HeaderText="Vista&nbsp;File" />
            
            <asp:BoundColumn DataField="Value" HeaderText="Sites" /> 
        </Columns>
    </asp:DataGrid>

    <h3 class="tableTitles">Errored Extractions (<asp:Label ID="labelErroredCount" runat="server" />):</h3>
    <asp:DataGrid ID="datagridErrored" AutoGenerateColumns="false" runat="server">
        <Columns>
            <asp:BoundColumn DataField="Key" HeaderText="Vista&nbsp;File" />
            
            <asp:BoundColumn DataField="Value" HeaderText="Sites" /> 
        </Columns>
    </asp:DataGrid>

</asp:Content>
