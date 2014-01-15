<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="EtlGui.aspx.cs" Inherits="com.bitscopic.downstream.EtlGui" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title></title>
</head>
<body>
    <form id="form1" runat="server">
    <div>
        <p><asp:Label ID="labelMessage" Font-Bold="true" ForeColor="Red" runat="server" /></p>
        
        <p>
            Select the environment:<br /><br />
            <asp:DropDownList ID="dropDownEnvironmentSelector" AutoPostBack="true" OnSelectedIndexChanged="dropDownEnvironmentClick" runat="server">
                <asp:ListItem Text="Dev" Value="dev" />
                <asp:ListItem Text="Pre-Prod" Value="pp" Selected="True" />
            </asp:DropDownList>
            &nbsp;&nbsp;&nbsp;Show only last ETL batch for a site: <asp:CheckBox ID="checkboxShowLastBatchOnly" runat="server" Checked="true" AutoPostBack="true" OnCheckedChanged="dropDownEnvironmentClick" />
        </p>

        <asp:DataGrid ID="dataGridEtlDownstreamMap" AutoGenerateColumns="false" runat="server" OnItemCommand="rerunClick">
            <AlternatingItemStyle BackColor="LightBlue" />
            <HeaderStyle BackColor="DarkBlue" ForeColor="White" Font-Bold="true" BorderColor="Black" />
            <Columns>
                <asp:BoundColumn DataField="EtlBatchId" HeaderText="ETL Batch ID" />

                <asp:BoundColumn DataField="DownstreamBatchId" HeaderText="Downstream Batch ID" />

                <asp:BoundColumn DataField="SiteID" HeaderText="Site ID" />

                <asp:TemplateColumn HeaderText="Domains">
                    <ItemTemplate>
                        <asp:CheckBox ID="checkboxPatAdt" runat="server" />&nbsp;Patient/ADT<br />
                        <asp:CheckBox ID="checkboxLabMicro" runat="server" />&nbsp;Microbiology&nbsp;Labs<br />
                        <asp:CheckBox ID="checkboxPharm" runat="server" />&nbsp;Pharmacy<br />
                        <asp:CheckBox ID="checkboxBcma" runat="server" />&nbsp;BCMA<br />
                        <asp:CheckBox ID="checkboxVitals" runat="server" />&nbsp;Vitals<br />
                    </ItemTemplate>
                </asp:TemplateColumn>

                <asp:ButtonColumn HeaderText="" ButtonType="PushButton" Text="Rerun Batch" CommandName="rerunClick" />
            </Columns>
        </asp:DataGrid>

        <p>
            <asp:Button ID="buttonViewLog" runat="server" Text="View Log" OnClick="ViewLogClick" />
            <br />
            <asp:TextBox ID="textBoxLog" Visible="false" Rows="50" Columns="120" runat="server" />
        </p>
    </div>
    </form>
</body>
</html>
