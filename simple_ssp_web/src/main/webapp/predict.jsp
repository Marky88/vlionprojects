<%--
  Created by IntelliJ IDEA.
  User: PC
  Date: 2021/4/30/0030
  Time: 17:28
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort() + path + "/";
%>

<html>
<head>
    <title>Title</title>
</head>
<script type="text/javascript" src="js/jquery-3.0.0.js"></script>
<script type="text/javascript" src="js/json.js"></script>
<script type="text/javascript">


    function query() {
        var queryall = $("#q").val();
        var url = encodeURI("<%=basePath %>queryServlet?queryParam=" + queryall + "&service=HDS&password=e10adc3949ba59abbe56e057f20f883e");
        $.ajaxSetup({
            timeout: 600000,
            cache: false,
            global: false,
            beforeSend: function () {
                $("#result").html("正在查询中......");
            }
        });
        $("#result").load(url, null, function (responseText, textStatus, XMLHttpRequest) {
            if (textStatus == "error") {
                $("#result").html("连接失败");
            } else if (textStatus == "timeout") {
                $("#result").html("查询超时");
            }
        });

    }
</script>

<body>

</body>
</html>
