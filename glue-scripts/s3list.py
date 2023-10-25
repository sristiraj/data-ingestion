from pretty_html_table import build_table

body = """
<html>
<head>
</head>

<body>
        {0}
</body>

</html>
""".format(build_table(df, 'blue_light'))
