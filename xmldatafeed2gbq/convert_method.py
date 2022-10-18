import ozon_method


def checkTypeFieldFloat(newdict, elfield):
    if newdict.__contains__(elfield) and type(newdict[elfield]) is not float:
        newdict[elfield] = ozon_method.parse_float(newdict[elfield])
