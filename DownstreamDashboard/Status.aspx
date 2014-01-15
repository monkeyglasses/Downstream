<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Status.aspx.cs" Inherits="DownstreamDashboard.Status" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <script src="Scripts/jquery-1.4.1.min.js" type="text/javascript"></script>
    <script src="Scripts/jquery.progressbar.js" type="text/javascript"></script>

    <script type="text/javascript">
        $(document).ready(function () {
            $(".progressBar").progressBar();
        });
    </script>
    <title>Extractor Status</title>
</head>
<body onload="javascript:window.resizeTo(500,300)">
    <form id="form1" runat="server">
    <div>

    
    <asp:Label ID="labelMessage" runat="server" ForeColor="Red" Font-Size="Larger" Font-Bold="true" />

    <br />
    <asp:Panel ID="panelGraph" runat="server">
        Percentage Complete: <span class="progressBar"><asp:Literal ID="literalPercentageComplete" runat="server" /></span>
    </asp:Panel>

    </div>
    </form>
</body>
</html>
