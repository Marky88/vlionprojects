<%@ page contentType="text/html;charset=UTF-8" language="java" pageEncoding="UTF-8"%>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />

<html>
<head>
    <title>mypic</title>
</head>


<body>

<% String[] picNames = (String[])request.getAttribute("picNames");


%>
<%--%>--%>
<%--<img src="<%="E:/新建文件夹/"+picNames[i]%>">--%>



<div>
<%for(int i =0;i<picNames.length;i++) {
    System.out.println("E:/pics/"+picNames[i]);
%>
    <img alt="pic" src="${pageContext.request.contextPath}/picservlet2?filePath=E:/pics/<%=picNames[i]%>}">
<%}%>
</div>

</body>
</html>
