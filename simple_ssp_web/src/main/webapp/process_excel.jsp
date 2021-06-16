<%--
  Created by IntelliJ IDEA.
  User: PC
  Date: 2021/3/23/0023
  Time: 15:54
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>process_zhifubao</title>
</head>
<body>
<h3 style="color: blue">  处理支付宝  </h3>
    <form action="upload_execel_zhifubao" method="post" enctype="multipart/form-data">
        <input type="file" name="myfile">
        <input type="submit" value="上传文件">
    </form>
<hr>

<h3 style="color: blue">  处理淘宝  </h3>
<form action="upload_execel_taobao" method="post" enctype="multipart/form-data">
    <input type="file" name="myfile">
    <input type="submit" value="上传文件">
</form>
</body>
</html>
