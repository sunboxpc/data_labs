update temp 
set md5 = cast(md5(CAST(temp.* as text)) as uuid)

