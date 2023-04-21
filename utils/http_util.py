from http.cookies import SimpleCookie


def ck_str_to_dict(ck:str):
    return dict(i.split('=', 1) for i in ck.split('; '))

def ck_dict_to_str(ck:dict):
    return "; ".join([str(x)+"="+str(y) for x,y in ck.items()])